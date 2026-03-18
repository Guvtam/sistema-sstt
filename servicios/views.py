import pandas as pd
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.urls import reverse
from django.db.models import Count, Sum, Q, Value
from django.db.models.functions import Coalesce
from decimal import Decimal
from datetime import datetime
import re
from .models import ServicioTecnico, CargaMensual, Contratista,CECO
from django.views.decorators.csrf import csrf_exempt
import json
from django.template.loader import render_to_string
from django.http import HttpResponse
from xhtml2pdf import pisa
from io import BytesIO
from django.db.models.functions import Trim, Upper



# ==============================
# FUNCIONES AUXILIARES
# ==============================

def convertir_fecha(valor):
    if valor is None:
        return None
    try:
        if isinstance(valor, datetime):
            return valor
        return datetime.strptime(str(valor), "%d/%m/%Y %H:%M:%S")
    except:
        return None


def limpiar_fecha(valor):
    if pd.isna(valor):
        return None
    return valor


def limpiar_nombre_tecnico(valor):

    if not valor:
        return None

    valor = valor.strip()

    valor = re.sub(
        r'^T\.?\s*INTERNO\s*-\s*',
        '',
        valor,
        flags=re.IGNORECASE
    )

    valor = re.sub(
        r'^T\.?\s*EXTERNO\s*-\s*',
        '',
        valor,
        flags=re.IGNORECASE
    )

    return valor.strip()


# ==============================
# PAGINAS BASE
# ==============================





def internos(request):

    # ==============================
    # CARGAS
    # ==============================

    cargas = CargaMensual.objects.all().order_by("-fecha_carga")

    tecnico_seleccionado = request.GET.get("tecnico")
    if tecnico_seleccionado == "None":
        tecnico_seleccionado = None

    carga_id = request.GET.get("carga")
    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    tipo_servicio = request.GET.get("tipo_servicio")

    # ==============================
    # SI NO VIENE MES/AÑO → USAR ÚLTIMA CARGA
    # ==============================

    if not mes or not anio:

        ultima_carga = CargaMensual.objects.order_by("-anio", "-mes").first()

        if ultima_carga:
            mes = str(ultima_carga.mes)
            anio = str(ultima_carga.anio)

    # ==============================
    # QUERY SERVICIOS
    # ==============================

    servicios = ServicioTecnico.objects.filter(
        contratista__isnull=True
    ).exclude(
        tecnico__iexact="Nexttech"
    )

    # filtro mes/año
    if mes and anio:
        servicios = servicios.filter(
            carga__mes=mes,
            carga__anio=anio
        )

    # filtro carga específica
    elif carga_id:
        servicios = servicios.filter(carga_id=carga_id)

    # filtro técnico
    if tecnico_seleccionado:
        servicios = servicios.filter(tecnico=tecnico_seleccionado)

    # filtro tipo servicio
    if tipo_servicio:
        servicios = servicios.filter(
            tipo_servicio__icontains=tipo_servicio
        )

    servicios = servicios.order_by("fecha_finalizacion")

    # ==============================
    # LISTA TECNICOS
    # ==============================

    tecnicos = ServicioTecnico.objects.filter(
        contratista__isnull=True
    ).exclude(
        tecnico__iexact="Nexttech"
    ).values_list(
        "tecnico", flat=True
    ).distinct().order_by("tecnico")

    # ==============================
    # RESUMEN
    # ==============================

    resumen = servicios.aggregate(

        total=Count("id"),

        preventivos=Count(
            "id",
            filter=Q(tipo_servicio__icontains="PREVENTIV")
        ),

        correctivos=Count(
            "id",
            filter=Q(tipo_servicio__icontains="CORRECTIV")
        ),
        costo_preventivas=Coalesce(
        Sum(
            "valor_pago_tecnico",
            filter=Q(tipo_servicio__icontains="PREVENTIV")
        ),
        Value(0)
        ),

        costo_correctivas=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(tipo_servicio__icontains="CORRECTIV")
            ),
            Value(0)
        ),


        costo_total=Coalesce(
            Sum("valor_pago_tecnico"),
            Value(0)
        )
    )
    
    # ==============================
    # RESUMEN POR TECNICO
    # ==============================

    resumen_tecnicos = servicios.values("tecnico").annotate(

        total=Count("id"),

        preventivas=Count(
            "id",
            filter=Q(tipo_servicio__icontains="PREVENTIV")
        ),

        correctivas=Count(
            "id",
            filter=Q(tipo_servicio__icontains="CORRECTIV")
        ),

        costo_total=Coalesce(
            Sum("valor_pago_tecnico"),
            Value(0)
        )

    ).exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).order_by("-total")


    # ==============================
    # SEPARAR SERVICIOS PRINCIPALES
    # ==============================

    servicios_principales = [
        "ServicioViña",
        "ServicioStgo",
        "Leonardo Flores",
    ]

    tabla_servicios = []
    tabla_tecnicos = []

    for tecnico in resumen_tecnicos:

        if tecnico["tecnico"] in servicios_principales:
            tabla_servicios.append(tecnico)

        else:
            tabla_tecnicos.append(tecnico)

    

    # ==============================
    # RENDER
    # ==============================

    return render(request, "servicios/internos.html", {

        "tecnicos": tecnicos,
        "cargas": cargas,
        "servicios": servicios,
        "resumen": resumen,
        "resumen_tecnicos": resumen_tecnicos,
        "tabla_servicios": tabla_servicios,
        "tabla_tecnicos": tabla_tecnicos,

        "tecnico_seleccionado": tecnico_seleccionado,
        "carga_id": carga_id,
        "tipo_servicio": tipo_servicio,

        "mes": mes,
        "anio": anio,

        
    })


# ==============================
# SUBIR ARCHIVO
# ==============================

def subir_excel(request):

    mensaje = ""

    if request.method == "POST":

        nombre_carga = request.POST.get("nombre_carga")
        mes = request.POST.get("mes")
        anio = request.POST.get("anio")
        archivo1 = request.FILES.get("archivo1")
        archivo2 = request.FILES.get("archivo2")

        if nombre_carga and archivo1 and archivo2:

            # eliminar carga anterior con mismo nombre
            CargaMensual.objects.filter(nombre=nombre_carga).delete()

            carga = CargaMensual.objects.create(
                nombre=nombre_carga,
                mes=mes,
                anio=anio,
                )

            dataframes = []

            for archivo in [archivo1, archivo2]:
                tablas = pd.read_html(archivo)
                if tablas:
                    dataframes.append(tablas[0])

            df = pd.concat(dataframes, ignore_index=True)

            df.columns = df.columns.str.strip()

            # ======================
            # LIMPIAR VALOR
            # ======================

            if "Valor pago técnico" in df.columns:

                df["Valor pago técnico"] = (
                    df["Valor pago técnico"]
                    .astype(str)
                    .str.replace(r"\.(?=\d{3})", "", regex=True)
                )

                df["Valor pago técnico"] = pd.to_numeric(
                    df["Valor pago técnico"],
                    errors="coerce"
                ).fillna(0).astype(int)

            # ======================
            # CUENTA CONTABLE
            # ======================

            if "Cuenta" in df.columns:

                df["cuenta_contable"] = (
                    df["Cuenta"]
                    .str.extract(r'(FIR\s*-\s*[A-Za-z0-9]+)')
                    .iloc[:, 0]
                    .str.replace(r'\s*-\s*', '-', regex=True)
                )

            # ======================
            # FECHA VISITA
            # ======================

            df["fecha_visita_convertida"] = pd.to_datetime(
                df.get("Fecha de la visita"),
                errors="coerce",
                dayfirst=True
            )

            df["fecha_visita_solo_dia"] = df["fecha_visita_convertida"].dt.date

            # ======================
            # INCIDENCIAS POR DIA
            # ======================

            df["numero_incidencias_dia"] = (
                df.groupby(
                    ["cuenta_contable", "fecha_visita_solo_dia"]
                )["cuenta_contable"]
                .transform("count")
            )

            df["numero_incidencias_dia"] = df["numero_incidencias_dia"].fillna(0)

            # ======================
            # GUARDAR
            # ======================

            for _, row in df.iterrows():

                nombre_tecnico_original = row.get("Tecnico")
                nombre_tecnico = None
                contratista_obj = None

                if nombre_tecnico_original and isinstance(nombre_tecnico_original, str):

                    nombre_tecnico_original = nombre_tecnico_original.strip()

                    nombre_tecnico = limpiar_nombre_tecnico(nombre_tecnico_original)

                    if nombre_tecnico_original.upper().startswith("T. EXTERNO") or nombre_tecnico_original.upper() == "NEXTTECH":

                        nombre_limpio = limpiar_nombre_tecnico(nombre_tecnico_original)

                        if nombre_limpio:
                            contratista_obj, created = Contratista.objects.get_or_create(
                                nombre=nombre_limpio
                            )

                # guardar valor original
                valor_pago = row.get("Valor pago técnico")

                ServicioTecnico.objects.create(

                    carga=carga,

                    numero=row.get("Número"),

                    fecha_creacion=convertir_fecha(row.get("Fecha de Creación")),
                    fecha_modificacion=convertir_fecha(row.get("Fecha de modificación")),

                    fecha_visita=limpiar_fecha(row.get("fecha_visita_convertida")),

                    fecha_finalizacion=convertir_fecha(row.get("Fecha de finalizacion")),

                    cuenta=row.get("Cuenta"),
                    cuenta_contable=row.get("cuenta_contable"),

                    telefono=row.get("Teléfono"),

                    tecnico=nombre_tecnico,
                    contratista=contratista_obj,

                    direccion=row.get("Dirección"),
                    provincia_estado=row.get("Provincia-Estado"),
                    localidad=row.get("Localidad"),

                    tipo_servicio=row.get("Tipo de Servicio"),
                    servicio=row.get("Servicio"),

                    observaciones=row.get("Observaciones (Insumos)"),
                    estado=row.get("Estado"),
                    usuario=row.get("Usuario"),

                    valor=row.get("Valor"),
                    costo_mano_obra=row.get("Costo mano de obra"),

                    fecha_pago=convertir_fecha(row.get("Fecha de pago")),

                    # NUEVOS CAMPOS
                    valor_pago_original=valor_pago,
                    valor_pago_tecnico=valor_pago,

                    tiempo_trabajo_total=row.get("Tiempo de Trabajo Total"),

                    numero_incidencias_dia=row.get("numero_incidencias_dia"),
                )

            mensaje = "Carga procesada correctamente"

    return render(request, "servicios/subir_excel.html", {
        "mensaje": mensaje
    })


# ==============================
# BUSCADOR
# ==============================

def buscador_servicios(request):

    query = request.GET.get("q")
    carga_id = request.GET.get("carga")
    mes = request.GET.get("mes")
    anio = request.GET.get("anio")

    # ==============================
    # SI NO VIENE MES/AÑO → USAR ÚLTIMA CARGA
    # ==============================

    if not mes or not anio:

        ultima_carga = CargaMensual.objects.order_by("-anio", "-mes").first()

        if ultima_carga:
            mes = str(ultima_carga.mes)
            anio = str(ultima_carga.anio)

    # ==============================
    # QUERY SERVICIOS
    # ==============================

    servicios = ServicioTecnico.objects.all().order_by("-fecha_finalizacion")

    # filtro por mes/año
    if mes and anio:
        servicios = servicios.filter(
            carga__mes=mes,
            carga__anio=anio
        )

    # filtro por carga
    elif carga_id:
        servicios = servicios.filter(carga_id=carga_id)

    # ==============================
    # BUSCADOR
    # ==============================

    if query:
        servicios = servicios.filter(
            Q(numero__icontains=query) |
            Q(cuenta_contable__icontains=query) |
            Q(cuenta__icontains=query) |
            Q(tecnico__icontains=query) |
            Q(contratista__nombre__icontains=query)
        )

    # ==============================
    # CARGAS
    # ==============================

    cargas = CargaMensual.objects.all().order_by("-fecha_carga")

    return render(request, "servicios/buscador.html", {
        "servicios": servicios,
        "query": query,
        "cargas": cargas,
        "carga_seleccionada": carga_id,
        "mes": mes,
        "anio": anio,
    })

# ==============================
# CONTRATISTAS
# ==============================





def contratista(request):

    cargas = CargaMensual.objects.all().order_by("-fecha_carga")

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    carga_id = request.GET.get("carga")
    contratista_id = request.GET.get("contratista_id")
    estado_pago = request.GET.get("estado_pago")
    tipo_servicio = request.GET.get("tipo_servicio")
    provincia_estado_sel = request.GET.get("provincia_estado")

    # ==============================
    # DEFAULT MES/AÑO
    # ==============================
    if not mes or not anio:
        ultima_carga = CargaMensual.objects.order_by("-anio", "-mes").first()
        if ultima_carga:
            mes = str(ultima_carga.mes)
            anio = str(ultima_carga.anio)

    # ==============================
    # CONTRATISTAS
    # ==============================
    contratistas = Contratista.objects.filter(
        servicios__isnull=False
    ).distinct().order_by("nombre")

    contratista_obj = None
    if contratista_id:
        contratista_obj = Contratista.objects.filter(id=contratista_id).first()

    # ==============================
    # QUERY BASE
    # ==============================
    servicios_qs = ServicioTecnico.objects.filter(
        contratista__isnull=False
    )

    if mes and anio:
        servicios_qs = servicios_qs.filter(
            carga__mes=mes,
            carga__anio=anio
        )
    elif carga_id:
        servicios_qs = servicios_qs.filter(carga_id=carga_id)

    if contratista_id:
        servicios_qs = servicios_qs.filter(contratista_id=contratista_id)

    if estado_pago:
        servicios_qs = servicios_qs.filter(estado_pago=estado_pago)

    if tipo_servicio:
        servicios_qs = servicios_qs.filter(
            tipo_servicio__icontains=tipo_servicio
        )

    servicios_qs = servicios_qs.order_by("fecha_finalizacion")

    # ==============================
    # 🔥 FILTRO ROBUSTO (INCLUYE NAN)
    # ==============================
    servicios = list(servicios_qs)

    if provincia_estado_sel:

        valor = provincia_estado_sel.strip().upper()

        servicios_filtrados = []

        for s in servicios:
            dato = str(s.provincia_estado).strip().upper() if s.provincia_estado else ""

            # 👉 caso NAN (texto)
            if valor == "NAN":
                if dato == "NAN" or dato == "":
                    servicios_filtrados.append(s)

            # 👉 caso normal
            else:
                if dato == valor:
                    servicios_filtrados.append(s)

        servicios = servicios_filtrados

    # ==============================
    # 🔥 SELECT (INCLUYE NAN)
    # ==============================
    provincias_set = set()

    for s in servicios_qs:
        dato = str(s.provincia_estado).strip().upper() if s.provincia_estado else "NAN"

        if not dato:
            dato = "NAN"

        provincias_set.add(dato)

    provincias = sorted(provincias_set)

    # =========================
    # CECO
    # =========================
    def normalizar(valor):
        if not valor:
            return ""
        return str(valor).strip().upper()

    ceco_dict = {
        normalizar(c.cuenta): c.ceco
        for c in CECO.objects.all()
    }

    for s in servicios:
        cuenta = normalizar(s.cuenta_contable)
        s.ceco = ceco_dict.get(cuenta, "-")

    # ==============================
    # RESUMEN
    # ==============================

    # ==============================
    # RESUMEN (LISTA FILTRADA)
    # ==============================

    total_mantenciones = len(servicios)

    total_preventivas = sum(
        1 for s in servicios
        if s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    )

    total_correctivas = sum(
        1 for s in servicios
        if s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    )

    cantidad_aprobado = sum(
        1 for s in servicios if s.estado_pago == "aprobado"
    )

    cantidad_revision = sum(
        1 for s in servicios if s.estado_pago == "revision"
    )

    cantidad_rechazado = sum(
        1 for s in servicios if s.estado_pago == "rechazado"
    )

    monto_total = sum(s.valor_pago_tecnico or 0 for s in servicios)

    monto_aprobado = sum(
        (s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "aprobado"
    )

    monto_revision = sum(
        (s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "revision"
    )

    monto_rechazado = sum(
        (s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "rechazado"
    )

    # Montos por tipo de servicio
    monto_preventivas = sum(
        s.valor_pago_tecnico or 0
        for s in servicios
        if s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    )

    monto_correctivas = sum(
        s.valor_pago_tecnico or 0
        for s in servicios
        if s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    )
    # PREV / CORR POR ESTADO

    aprobado_preventivas = sum(
        1 for s in servicios
        if s.estado_pago == "aprobado"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    )

    aprobado_correctivas = sum(
        1 for s in servicios
        if s.estado_pago == "aprobado"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    )

    revision_preventivas = sum(
        1 for s in servicios
        if s.estado_pago == "revision"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    )

    revision_correctivas = sum(
        1 for s in servicios
        if s.estado_pago == "revision"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    )

    rechazado_preventivas = sum(
        1 for s in servicios
        if s.estado_pago == "rechazado"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    )

    rechazado_correctivas = sum(
        1 for s in servicios
        if s.estado_pago == "rechazado"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    )



    # Montos por clasificación
    total_b2b = sum(1 for s in servicios if s.clasificacion == "B2B")
    total_b2c = sum(1 for s in servicios if s.clasificacion == "B2C")

    monto_b2b = sum(s.valor_pago_tecnico or 0 for s in servicios if s.clasificacion == "B2B")
    monto_b2c = sum(s.valor_pago_tecnico or 0 for s in servicios if s.clasificacion == "B2C")

    total_mantenciones = len(servicios)
    monto_total = sum(
        s.valor_pago_tecnico or 0 for s in servicios
    )

    resumen = {
        
        
    
        "total_mantenciones": total_mantenciones,
        "total_preventivas": total_preventivas,
        "total_correctivas": total_correctivas,

        "cantidad_aprobado": cantidad_aprobado,
        "cantidad_revision": cantidad_revision,
        "cantidad_rechazado": cantidad_rechazado,

        "monto_total": monto_total,
        "monto_aprobado": monto_aprobado,
        "monto_revision": monto_revision,
        "monto_rechazado": monto_rechazado,

        "monto_preventivas": monto_preventivas,
        "monto_correctivas": monto_correctivas,


        "aprobado_preventivas": aprobado_preventivas,
        "aprobado_correctivas": aprobado_correctivas,

        "revision_preventivas": revision_preventivas,
        "revision_correctivas": revision_correctivas,

        "rechazado_preventivas": rechazado_preventivas,
        "rechazado_correctivas": rechazado_correctivas,

        "total_b2b": total_b2b,
        "total_b2c": total_b2c,
        "monto_b2b": monto_b2b,
        "monto_b2c": monto_b2c,
    }

    return render(request, "servicios/contratista.html", {
        "cargas": cargas,
        "contratistas": contratistas,
        "servicios": servicios,
        "contratista": contratista_obj,
        "resumen": resumen,
        "mes": mes,
        "anio": anio,
        "carga_id": carga_id,
        "contratista_id": contratista_id,
        "estado_pago": estado_pago,
        "tipo_servicio": tipo_servicio,
        "provincia_estado_sel": provincia_estado_sel,
        "provincia": provincias,
    })



# ==============================
# CAMBIAR ESTADO PAGO
# ==============================

def cambiar_estado_pago(request, servicio_id):
    servicio = get_object_or_404(ServicioTecnico, id=servicio_id)

    if request.method == "POST":
        servicio.estado_pago = request.POST.get("estado_pago")
        servicio.save()

    servicios = ServicioTecnico.objects.filter(contratista=servicio.contratista)

    resumen = servicios.aggregate(
        total_mantenciones=Count("id"),
        monto_total=Coalesce(Sum("valor_pago_tecnico"), Value(0)),
        monto_preventivas=Coalesce(Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="PREVENTIV")), Value(0)),
        monto_correctivas=Coalesce(Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="CORRECTIV")), Value(0)),
        monto_aprobado=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="aprobado")), Value(0)),
        monto_revision=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="revision")), Value(0)),
        monto_rechazado=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="rechazado")), Value(0)),
        cantidad_aprobado=Count("id", filter=Q(estado_pago="aprobado")),
        cantidad_revision=Count("id", filter=Q(estado_pago="revision")),
        cantidad_rechazado=Count("id", filter=Q(estado_pago="rechazado")),
    )

    return JsonResponse({"success": True, "resumen": resumen})


# ==============================
# ACTUALIZAR MONTO
# ==============================

def actualizar_monto(request, servicio_id):

    if request.method == "POST":

        servicio = get_object_or_404(
            ServicioTecnico,
            id=servicio_id
        )

        nuevo_valor = request.POST.get("valor")

        try:

            servicio.valor_pago_tecnico = Decimal(nuevo_valor)

            servicio.save()

            return JsonResponse({"success": True})

        except:

            return JsonResponse({"success": False})

    return JsonResponse({"success": False})


# ==============================
# ELIMINAR CARGA COMPLETA
# ==============================

def eliminar_carga(request, carga_id):

    carga = get_object_or_404(CargaMensual, id=carga_id)

    carga.delete()

    return redirect("inicio")




@csrf_exempt
def editar_monto_tecnico(request, servicio_id):

    if request.method == "POST":

        try:

            data = json.loads(request.body)
            nuevo_valor = float(data.get("valor_pago_tecnico", 0))

            servicio = ServicioTecnico.objects.get(id=servicio_id)

            # GUARDAR VALOR ORIGINAL SOLO UNA VEZ
            if servicio.valor_pago_original is None:
                servicio.valor_pago_original = servicio.valor_pago_tecnico

            # ACTUALIZAR VALOR EDITADO
            servicio.valor_pago_tecnico = nuevo_valor
            servicio.save()

            # recalcular resumen del contratista
            contratista = servicio.contratista

            servicios = ServicioTecnico.objects.filter(
                contratista=contratista
            )

            resumen = servicios.aggregate(

                total_mantenciones=Count("id"),

                total_preventivas=Count(
                    "id",
                    filter=Q(tipo_servicio__icontains="PREVENTIV")
                ),

                total_correctivas=Count(
                    "id",
                    filter=Q(tipo_servicio__icontains="CORRECTIV")
                ),

                monto_total=Coalesce(
                    Sum("valor_pago_tecnico"),
                    0
                ),

                monto_aprobado=Coalesce(
                    Sum(
                        "valor_pago_tecnico",
                        filter=Q(estado_pago="aprobado")
                    ),
                    0
                ),

                monto_revision=Coalesce(
                    Sum(
                        "valor_pago_tecnico",
                        filter=Q(estado_pago="revision")
                    ),
                    0
                ),

                monto_rechazado=Coalesce(
                    Sum(
                        "valor_pago_tecnico",
                        filter=Q(estado_pago="rechazado")
                    ),
                    0
                ),
            )

            return JsonResponse({
                "success": True,
                "resumen": resumen
            })

        except Exception as e:

            return JsonResponse({
                "success": False,
                "error": str(e)
            })

    return JsonResponse({
        "success": False,
        "error": "Método no permitido"
    })





def obtener_ceco(servicio):
    return CECO.objects.filter(
        cuenta=servicio.cuenta_contable,
        ceco=servicio.ceco_codigo
    ).first()




def contratista_pdf(request):

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    carga_id = request.GET.get("carga")
    contratista_id = request.GET.get("contratista_id")
    estado_pago = request.GET.get("estado_pago")
    tipo_servicio = request.GET.get("tipo_servicio")

    # ==============================
    # CONTRATISTA SELECCIONADO
    # ==============================

    contratista_obj = None
    if contratista_id:
        contratista_obj = Contratista.objects.filter(id=contratista_id).first()

    # ==============================
    # QUERY SERVICIOS (IGUAL QUE LA VISTA PRINCIPAL)
    # ==============================

    servicios = ServicioTecnico.objects.filter(
        contratista__isnull=False
    )

    # filtro mes/año
    if mes and anio:
        servicios = servicios.filter(
            carga__mes=mes,
            carga__anio=anio
        )

    # filtro carga específica
    elif carga_id:
        servicios = servicios.filter(carga_id=carga_id)

    # filtro contratista
    if contratista_id:
        servicios = servicios.filter(contratista_id=contratista_id)

    # filtro estado pago
    if estado_pago:
        servicios = servicios.filter(estado_pago=estado_pago)

    # filtro tipo servicio
    if tipo_servicio:
        servicios = servicios.filter(
            tipo_servicio__icontains=tipo_servicio
        )

    servicios = servicios.order_by("fecha_finalizacion")

    # ==============================
    # RESUMEN (IGUAL QUE LA VISTA)
    # ==============================

    resumen = servicios.aggregate(

        total_mantenciones=Count("id"),

        total_preventivas=Count(
            "id",
            filter=Q(tipo_servicio__icontains="PREVENTIV")
        ),

        total_correctivas=Count(
            "id",
            filter=Q(tipo_servicio__icontains="CORRECTIV")
        ),

        cantidad_aprobado=Count(
            "id",
            filter=Q(estado_pago="aprobado")
        ),

        cantidad_revision=Count(
            "id",
            filter=Q(estado_pago="revision")
        ),

        cantidad_rechazado=Count(
            "id",
            filter=Q(estado_pago="rechazado")
        ),

        monto_preventivas=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(tipo_servicio__icontains="PREVENTIV")
            ),
            Value(0)
        ),

        monto_correctivas=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(tipo_servicio__icontains="CORRECTIV")
            ),
            Value(0)
        ),

        monto_aprobado=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(estado_pago="aprobado")
            ),
            Value(0)
        ),

        monto_revision=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(estado_pago="revision")
            ),
            Value(0)
        ),

        monto_rechazado=Coalesce(
            Sum(
                "valor_pago_tecnico",
                filter=Q(estado_pago="rechazado")
            ),
            Value(0)
        ),

        monto_total=Coalesce(
            Sum("valor_pago_tecnico"),
            Value(0)
        ),
    )

    # ==============================
    # GENERAR HTML
    # ==============================

    html_string = render_to_string(
        "servicios/contratista_pdf.html",
        {
            "contratista": contratista_obj,
            "servicios": servicios,
            "resumen": resumen,
            "estado_pago": estado_pago,
            "mes": mes,
            "anio": anio,
        }
    )

    # ==============================
    # GENERAR PDF
    # ==============================

    buffer = BytesIO()

    pisa.CreatePDF(
        html_string,
        dest=buffer
    )

    buffer.seek(0)

    response = HttpResponse(
        buffer,
        content_type="application/pdf"
    )

    response["Content-Disposition"] = \
        'inline; filename="reporte_contratista.pdf"'

    return response





def exportar_excel(request):

    servicios = ServicioTecnico.objects.all().values()

    df = pd.DataFrame(servicios)

    # 🔥 SOLUCIÓN DEFINITIVA
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=servicios.xlsx'

    df.to_excel(response, index=False)

    return response
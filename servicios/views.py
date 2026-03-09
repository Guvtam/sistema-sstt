
import pandas as pd
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.urls import reverse
from django.db.models import Count, Sum, Q, Value
from django.db.models.functions import Coalesce
from decimal import Decimal
from datetime import datetime
import re
from .models import ServicioTecnico, CargaMensual, Contratista
from django.views.decorators.csrf import csrf_exempt
import json



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

def inicio(request):
    return render(request, "servicios/inicio.html")




def internos(request):

    tecnicos = ServicioTecnico.objects.filter(
        contratista__isnull=True
    ).exclude(
        tecnico__iexact="Nexttech"
    ).values_list(
        "tecnico",
        flat=True
    ).distinct().order_by("tecnico")

    tecnico_seleccionado = request.GET.get("tecnico")

    servicios = None
    resumen = None

    if tecnico_seleccionado:

        servicios = ServicioTecnico.objects.filter(
        tecnico=tecnico_seleccionado,
        contratista__isnull=True
    ).exclude(
        tecnico__iexact="Nexttech"
    ).order_by("numero")

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

            costo_total=Coalesce(
                Sum("valor_pago_tecnico"),
                Value(0)
            )
        )

    return render(request, "servicios/internos.html", {

        "tecnicos": tecnicos,
        "servicios": servicios,
        "resumen": resumen,
        "tecnico_seleccionado": tecnico_seleccionado

    })

# ==============================
# SUBIR ARCHIVO
# ==============================

def subir_excel(request):

    mensaje = ""

    if request.method == "POST":

        nombre_carga = request.POST.get("nombre_carga")
        archivo1 = request.FILES.get("archivo1")
        archivo2 = request.FILES.get("archivo2")

        if nombre_carga and archivo1 and archivo2:

            # eliminar carga anterior con mismo nombre
            CargaMensual.objects.filter(nombre=nombre_carga).delete()

            carga = CargaMensual.objects.create(nombre=nombre_carga)

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

                    if nombre_tecnico_original.upper().startswith("T. EXTERNO"):

                        nombre_limpio = limpiar_nombre_tecnico(nombre_tecnico_original)

                        if nombre_limpio:

                            contratista_obj, created = Contratista.objects.get_or_create(
                                nombre=nombre_limpio
                            )

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

                    valor_pago_tecnico=row.get("Valor pago técnico"),

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

    servicios = ServicioTecnico.objects.all().order_by("numero")

    if query:

        servicios = servicios.filter(

            Q(tecnico__icontains=query) |
            Q(numero__icontains=query) |
            Q(cuenta_contable__icontains=query)

        )

    return render(request, "servicios/buscador.html", {
        "servicios": servicios,
        "query": query
    })


# ==============================
# CONTRATISTAS
# ==============================

from django.db.models import Count, Sum, Q, Value
from django.db.models.functions import Coalesce
from django.shortcuts import render, get_object_or_404

def contratista(request):

    contratistas = Contratista.objects.all()
    contratista_id = request.GET.get("contratista_id")

    servicios = None
    resumen = None
    contratista = None

    if contratista_id:

        contratista = get_object_or_404(Contratista, id=contratista_id)

        servicios = ServicioTecnico.objects.filter(
            contratista=contratista
        ).order_by("numero")

        resumen = servicios.aggregate(
            # Cantidades
            total_mantenciones=Count("id"),
            total_correctivas=Count(
                "id",
                filter=Q(tipo_servicio__icontains="correct")
            ),
            total_preventivas=Count(
                "id",
                filter=Q(tipo_servicio__icontains="prevent")
            ),

            # Montos totales por tipo
            monto_correctivas=Coalesce(
                Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="correct")),
                Value(0)
            ),
            monto_preventivas=Coalesce(
                Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="prevent")),
                Value(0)
            ),

            # Montos por estado de pago
            monto_aprobado=Coalesce(
                Sum("valor_pago_tecnico", filter=Q(estado_pago="aprobado")),
                Value(0)
            ),
            monto_revision=Coalesce(
                Sum("valor_pago_tecnico", filter=Q(estado_pago="revision")),
                Value(0)
            ),
            monto_rechazado=Coalesce(
                Sum("valor_pago_tecnico", filter=Q(estado_pago="rechazado")),
                Value(0)
            ),

            # Cantidad por estado de pago
            cantidad_aprobado=Count("id", filter=Q(estado_pago="aprobado")),
            cantidad_revision=Count("id", filter=Q(estado_pago="revision")),
            cantidad_rechazado=Count("id", filter=Q(estado_pago="rechazado")),

            # Total general
            monto_total=Coalesce(Sum("valor_pago_tecnico"), Value(0)),
        )

    return render(request, "servicios/contratista.html", {
        "contratistas": contratistas,
        "servicios": servicios,
        "contratista": contratista,
        "resumen": resumen,
        "contratista_id": contratista_id,
    })

# ==============================
# CAMBIAR ESTADO PAGO
# ==============================

def cambiar_estado_pago(request, servicio_id):

    servicio = get_object_or_404(ServicioTecnico, id=servicio_id)

    if request.method == "POST":

        servicio.estado_pago = request.POST.get("estado_pago")

        servicio.save()

    contratista_id = servicio.contratista_id

    return redirect(
        f"{reverse('contratista')}?contratista_id={contratista_id}#tabla-servicios"
    )


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
            servicio.valor_pago_tecnico = nuevo_valor
            servicio.save()

            # recalcular resumen del contratista
            contratista = servicio.contratista
            servicios = ServicioTecnico.objects.filter(contratista=contratista)

            resumen = servicios.aggregate(
                total_mantenciones=Count("id"),
                total_preventivas=Count("id", filter=Q(tipo_servicio__icontains="PREVENTIV")),
                total_correctivas=Count("id", filter=Q(tipo_servicio__icontains="CORRECTIV")),
                monto_total=Coalesce(Sum("valor_pago_tecnico"), 0),
                monto_aprobado=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="aprobado")), 0),
                monto_revision=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="revision")), 0),
                monto_rechazado=Coalesce(Sum("valor_pago_tecnico", filter=Q(estado_pago="rechazado")), 0),
            )

            return JsonResponse({"success": True, "resumen": resumen})

        except Exception as e:
            return JsonResponse({"success": False, "error": str(e)})
    return JsonResponse({"success": False, "error": "Método no permitido"})
import json
import logging
import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO

import pandas as pd
from django.db import transaction
from django.db.models import Count, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from xhtml2pdf import pisa

from .models import CECO, CargaMensual, Contratista, ServicioTecnico, Tecnico, CuentaB2B

logger = logging.getLogger(__name__)


# ==============================
# FUNCIONES AUXILIARES
# ==============================

def quitar_tildes(texto):
    if not texto:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def normalizar_nombre(valor):
    if not valor:
        return ""

    valor = quitar_tildes(valor)
    valor = valor.upper().strip()
    valor = valor.replace(".", "")

    valor = re.sub(r"\bEIRL\b", "", valor)
    valor = re.sub(r"\bSPA\b", "", valor)
    valor = re.sub(r"\bLTDA\b", "", valor)
    valor = re.sub(r"\bLIMITADA\b", "", valor)

    valor = re.sub(r"^T\s*INTERNO\s*", "", valor)
    valor = re.sub(r"^T\s*EXTERNO\s*", "", valor)

    valor = re.sub(r"[^A-Z0-9 ]", "", valor)
    valor = re.sub(r"\s+", " ", valor)

    return valor.strip()


def limpiar_nombre_tecnico(valor):
    if valor is None:
        return None

    valor = str(valor).strip()

    if valor.upper() in ["", "NULL", "NONE", "NAN"]:
        return None

    valor = re.sub(r"^T\.?\s*INTERNO\s*-\s*", "", valor, flags=re.IGNORECASE)
    valor = re.sub(r"^T\.?\s*EXTERNO\s*-\s*", "", valor, flags=re.IGNORECASE)

    return valor.strip()


def normalizar_cuenta(valor):
    if valor is None:
        return ""

    valor = str(valor).strip().upper()

    if valor in ["", "NAN", "NONE", "NULL"]:
        return ""

    # quitar todos los espacios
    valor = re.sub(r"\s+", "", valor)

    # FIR-005C / FIR005C / FIR_005C -> FIR-005C
    match_fir = re.match(r"^FIR[-_]?([A-Z0-9]+)$", valor)
    if match_fir:
        return f"FIR-{match_fir.group(1)}"

    # NEW-SF_99 / NEWSF99 / NEW-SF-99 / NEWSF_99 -> NEW-SF_99
    match_new = re.match(r"^NEW[-_]?SF[-_]?([A-Z0-9]+)$", valor)
    if match_new:
        return f"NEW-SF_{match_new.group(1)}"

    return valor


def extraer_cuenta_contable(valor):
    if valor is None:
        return ""

    texto = str(valor).strip().upper()

    if texto in ["", "NAN", "NONE", "NULL"]:
        return ""

    # Buscar FIR
    match = re.search(r"FIR\s*-?\s*[A-Z0-9]+", texto)
    if match:
        return normalizar_cuenta(match.group(0))

    # Buscar NEW-SF
    match = re.search(r"NEW\s*-?\s*SF[_-]?\s*[A-Z0-9]+", texto)
    if match:
        return normalizar_cuenta(match.group(0))

    return ""


def clasificar_b2b(cuenta, cuentas_b2b_set):
    cuenta_extraida = extraer_cuenta_contable(cuenta)
    cuenta_normalizada = normalizar_cuenta(cuenta_extraida)
    return cuenta_normalizada in cuentas_b2b_set, cuenta_extraida


def convertir_fecha(valor):
    if valor is None or valor == "":
        return None

    try:
        if pd.isna(valor):
            return None
    except Exception:
        pass

    try:
        if isinstance(valor, pd.Timestamp):
            if pd.isna(valor):
                return None
            fecha = valor.to_pydatetime()

        elif isinstance(valor, datetime):
            fecha = valor

        else:
            texto = str(valor).strip()

            if texto.upper() in ["", "NAN", "NONE", "NULL"]:
                return None

            fecha = None
            for formato in [
                "%d/%m/%Y %H:%M:%S",
                "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%d-%m-%Y %H:%M",
                "%d/%m/%Y",
                "%d-%m-%Y",
            ]:
                try:
                    fecha = datetime.strptime(texto, formato)
                    break
                except ValueError:
                    pass

            if fecha is None:
                fecha = pd.to_datetime(texto, errors="coerce", dayfirst=True)
                if pd.isna(fecha):
                    return None
                fecha = fecha.to_pydatetime()

        if timezone.is_naive(fecha):
            fecha = timezone.make_aware(fecha)

        return fecha

    except Exception:
        return None


def limpiar_fecha(valor):
    if valor is None:
        return None

    try:
        if pd.isna(valor):
            return None
    except Exception:
        pass

    if isinstance(valor, pd.Timestamp):
        valor = valor.to_pydatetime()

    if isinstance(valor, datetime):
        if timezone.is_naive(valor):
            return timezone.make_aware(valor)
        return valor

    return None





def obtener_resumen_contratista(servicios):
    numeros_totales = {s.numero for s in servicios if s.numero is not None}

    numeros_preventivas = {
        s.numero for s in servicios
        if s.numero is not None and s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    }

    numeros_correctivas = {
        s.numero for s in servicios
        if s.numero is not None and s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    }

    numeros_aprobado = {
        s.numero for s in servicios
        if s.numero is not None and s.estado_pago == "aprobado"
    }

    numeros_revision = {
        s.numero for s in servicios
        if s.numero is not None and s.estado_pago == "revision"
    }

    numeros_rechazado = {
        s.numero for s in servicios
        if s.numero is not None and s.estado_pago == "rechazado"
    }

    numeros_aprobado_preventivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "aprobado"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    }

    numeros_aprobado_correctivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "aprobado"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    }

    numeros_revision_preventivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "revision"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    }

    numeros_revision_correctivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "revision"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    }

    numeros_rechazado_preventivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "rechazado"
        and s.tipo_servicio
        and "PREVENTIV" in s.tipo_servicio.upper()
    }

    numeros_rechazado_correctivas = {
        s.numero for s in servicios
        if s.numero is not None
        and s.estado_pago == "rechazado"
        and s.tipo_servicio
        and "CORRECTIV" in s.tipo_servicio.upper()
    }

    numeros_b2b = {
        s.numero for s in servicios
        if s.numero is not None and s.es_b2b
    }

    numeros_b2c = {
        s.numero for s in servicios
        if s.numero is not None and not s.es_b2b
    }

    monto_total = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios))
    monto_aprobado = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios if s.estado_pago == "aprobado"))
    monto_revision = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios if s.estado_pago == "revision"))
    monto_rechazado = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios if s.estado_pago == "rechazado"))

    monto_preventivas = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios
        if s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    ))

    monto_correctivas = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios
        if s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    ))

    monto_b2b = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios if s.es_b2b))
    monto_b2c = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios if not s.es_b2b))

    iva_19 = monto_total * Decimal("0.19")
    monto_total_con_iva = monto_total + iva_19

    return {
        "total_mantenciones": len(numeros_totales),
        "total_preventivas": len(numeros_preventivas),
        "total_correctivas": len(numeros_correctivas),
        "cantidad_aprobado": len(numeros_aprobado),
        "cantidad_revision": len(numeros_revision),
        "cantidad_rechazado": len(numeros_rechazado),
        "monto_total": monto_total,
        "monto_aprobado": monto_aprobado,
        "monto_revision": monto_revision,
        "monto_rechazado": monto_rechazado,
        "monto_preventivas": monto_preventivas,
        "monto_correctivas": monto_correctivas,
        "aprobado_preventivas": len(numeros_aprobado_preventivas),
        "aprobado_correctivas": len(numeros_aprobado_correctivas),
        "revision_preventivas": len(numeros_revision_preventivas),
        "revision_correctivas": len(numeros_revision_correctivas),
        "rechazado_preventivas": len(numeros_rechazado_preventivas),
        "rechazado_correctivas": len(numeros_rechazado_correctivas),
        "total_b2b": len(numeros_b2b),
        "total_b2c": len(numeros_b2c),
        "monto_b2b": monto_b2b,
        "monto_b2c": monto_b2c,
        "iva_19": iva_19,
        "monto_total_con_iva": monto_total_con_iva,
    }


def obtener_resumen_estado_qs(servicios):
    return servicios.aggregate(
        total_mantenciones=Count("numero", distinct=True),
        monto_total=Coalesce(Sum("valor_pago_tecnico"), Value(0)),
        monto_preventivas=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="PREVENTIV")),
            Value(0)
        ),
        monto_correctivas=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="CORRECTIV")),
            Value(0)
        ),
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
        cantidad_aprobado=Count("numero", distinct=True, filter=Q(estado_pago="aprobado")),
        cantidad_revision=Count("numero", distinct=True, filter=Q(estado_pago="revision")),
        cantidad_rechazado=Count("numero", distinct=True, filter=Q(estado_pago="rechazado")),
        aprobado_preventivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="aprobado", tipo_servicio__icontains="PREVENTIV")
        ),
        aprobado_correctivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="aprobado", tipo_servicio__icontains="CORRECTIV")
        ),
        revision_preventivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="revision", tipo_servicio__icontains="PREVENTIV")
        ),
        revision_correctivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="revision", tipo_servicio__icontains="CORRECTIV")
        ),
        rechazado_preventivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="rechazado", tipo_servicio__icontains="PREVENTIV")
        ),
        rechazado_correctivas=Count(
            "numero",
            distinct=True,
            filter=Q(estado_pago="rechazado", tipo_servicio__icontains="CORRECTIV")
        ),
    )


def obtener_ceco(servicio):
    return CECO.objects.filter(
        cuenta=servicio.cuenta_contable,
        ceco=getattr(servicio, "ceco_codigo", None)
    ).first()


# ==============================
# PÁGINAS BASE
# ==============================

def internos(request):
    cargas = CargaMensual.objects.all().order_by("-fecha_carga")

    tecnico_seleccionado = request.GET.get("tecnico")
    carga_id = request.GET.get("carga")
    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    tipo_servicio = request.GET.get("tipo_servicio")

    servicios = ServicioTecnico.objects.filter(
        tecnico_obj__tipo="interno"
    ).select_related("tecnico_obj", "contratista", "carga")

    if mes and anio:
        servicios = servicios.filter(carga__mes=mes, carga__anio=anio)
    elif carga_id:
        servicios = servicios.filter(carga_id=carga_id)

    if tecnico_seleccionado:
        servicios = servicios.filter(tecnico=tecnico_seleccionado)

    if tipo_servicio:
        servicios = servicios.filter(tipo_servicio__icontains=tipo_servicio)

    servicios = servicios.order_by("fecha_finalizacion")

    tecnicos = ServicioTecnico.objects.filter(
        Q(tecnico_obj__tipo="interno") |
        Q(tecnico_obj__isnull=True, contratista__isnull=True)
    ).exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).values_list(
        "tecnico", flat=True
    ).distinct().order_by("tecnico")

    resumen = servicios.aggregate(
        total=Count("numero", distinct=True),

        preventivos=Count("numero", distinct=True, filter=Q(tipo_servicio__icontains="PREVENTIV")),
        correctivos=Count("numero", distinct=True, filter=Q(tipo_servicio__icontains="CORRECTIV")),

        total_b2b=Count("numero", distinct=True, filter=Q(es_b2b=True)),
        total_b2c=Count("numero", distinct=True, filter=Q(es_b2b=False)),

        costo_preventivas=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="PREVENTIV")),
            Value(0)
        ),
        costo_correctivas=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(tipo_servicio__icontains="CORRECTIV")),
            Value(0)
        ),

        costo_b2b=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(es_b2b=True)),
            Value(0)
        ),
        costo_b2c=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(es_b2b=False)),
            Value(0)
        ),

        costo_total=Coalesce(Sum("valor_pago_tecnico"), Value(0))
    )

    resumen_tecnicos = servicios.values("tecnico").annotate(
        total=Count("numero", distinct=True),

        b2b=Count("numero", distinct=True, filter=Q(es_b2b=True)),
        b2c=Count("numero", distinct=True, filter=Q(es_b2b=False)),

        costo_b2b=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(es_b2b=True)),
            Value(0)
        ),
        costo_b2c=Coalesce(
            Sum("valor_pago_tecnico", filter=Q(es_b2b=False)),
            Value(0)
        ),

        costo_total=Coalesce(Sum("valor_pago_tecnico"), Value(0))
    ).exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).order_by("-costo_total")

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

    ALIAS_TECNICOS = {
        "ROBERTO BARRERA N": "ROBERTO BARRERA N",
        "ROMER PEREZ": "ROMER PEREZ",
        "MARCELO COLQUE ULLOA": "MARCELO COLQUE ULLOA",
        "NEGTEL INGENIERIA": "NEGTEL INGENIERIA SPA",
        "NEGTEL INGENIERIA SPA": "NEGTEL INGENIERIA SPA",
        "NELSON VERA EIRL": "NELSON VERA EIRL",
        "JR SYSTEM SECURITY": "JR SYSTEM SECURITY",
        "DEFCON": "DEFCON",
        "SERVICIOSTGO": "SERVICIOSTGO",
        "SERVICIOVINA": "SERVICIOVINA",
    }

    CONTRATISTAS_MANUALES = {
        "PABLO ALVARADO",
    }

    if request.method == "POST":
        nombre_carga = request.POST.get("nombre_carga")
        mes = request.POST.get("mes")
        anio = request.POST.get("anio")
        archivo1 = request.FILES.get("archivo1")
        archivo2 = request.FILES.get("archivo2")

        if nombre_carga and archivo1 and archivo2:
            try:
                # Primero validar lectura antes de borrar datos anteriores
                dataframes = []

                for archivo in [archivo1, archivo2]:
                    archivo.seek(0)
                    tablas = pd.read_html(archivo)
                    if tablas:
                        dataframes.append(tablas[0])

                if not dataframes:
                    raise ValueError("No se encontraron tablas válidas en los archivos.")

                df = pd.concat(dataframes, ignore_index=True)
                df.columns = df.columns.astype(str).str.strip()

                with transaction.atomic():
                    # Reemplazar solo cuando ya está validado el DataFrame
                    CargaMensual.objects.filter(nombre=nombre_carga).delete()

                    carga = CargaMensual.objects.create(
                        nombre=nombre_carga,
                        mes=int(mes) if mes else None,
                        anio=int(anio) if anio else None,
                    )

                    # limpiar monto pago técnico
                    if "Valor pago técnico" in df.columns:
                        df["Valor pago técnico"] = (
                            df["Valor pago técnico"]
                            .astype(str)
                            .str.replace(r"\.(?=\d{3})", "", regex=True)
                            .str.replace(",", "", regex=False)
                            .str.strip()
                        )
                        df["Valor pago técnico"] = pd.to_numeric(
                            df["Valor pago técnico"],
                            errors="coerce"
                        ).fillna(0)

                    # limpiar valor general
                    if "Valor" in df.columns:
                        df["Valor"] = (
                            df["Valor"]
                            .astype(str)
                            .str.replace(r"\.(?=\d{3})", "", regex=True)
                            .str.replace(",", "", regex=False)
                            .str.strip()
                        )
                        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

                    # limpiar costo mano de obra
                    if "Costo mano de obra" in df.columns:
                        df["Costo mano de obra"] = (
                            df["Costo mano de obra"]
                            .astype(str)
                            .str.replace(r"\.(?=\d{3})", "", regex=True)
                            .str.replace(",", "", regex=False)
                            .str.strip()
                        )
                        df["Costo mano de obra"] = pd.to_numeric(
                            df["Costo mano de obra"],
                            errors="coerce"
                        )

                    # cuenta contable desde cuenta original
                    if "Cuenta" in df.columns:
                        df["cuenta_contable"] = df["Cuenta"].apply(extraer_cuenta_contable)
                    else:
                        df["cuenta_contable"] = ""

                    # fecha visita
                    if "Fecha de la visita" in df.columns:
                        df["fecha_visita_convertida"] = pd.to_datetime(
                            df["Fecha de la visita"],
                            errors="coerce",
                            dayfirst=True
                        )
                    else:
                        df["fecha_visita_convertida"] = pd.NaT

                    df["fecha_visita_solo_dia"] = df["fecha_visita_convertida"].dt.date

                    # incidencias por día
                    df["numero_incidencias_dia"] = (
                        df.groupby(
                            ["cuenta_contable", "fecha_visita_solo_dia"],
                            dropna=False
                        )["cuenta_contable"]
                        .transform("count")
                        .fillna(0)
                        .astype(int)
                    )

                    # diccionario de contratistas
                    contratistas_db = {}
                    for c in Contratista.objects.all():
                        if c.nombre:
                            contratistas_db[normalizar_nombre(c.nombre)] = c
                        if c.nombre_empresa:
                            contratistas_db[normalizar_nombre(c.nombre_empresa)] = c

                    # diccionario de técnicos
                    tecnicos_db = {}
                    for t in Tecnico.objects.select_related("contratista").all():
                        if t.nombre:
                            tecnicos_db[normalizar_nombre(t.nombre)] = t

                    # set de cuentas b2b normalizadas
                    cuentas_b2b_set = {
                        normalizar_cuenta(c)
                        for c in CuentaB2B.objects.values_list("cuenta", flat=True)
                        if c
                    }

                    servicios_a_crear = []

                    for _, row in df.iterrows():
                        nombre_original = row.get("Tecnico")
                        nombre_limpio = limpiar_nombre_tecnico(nombre_original)
                        nombre_normalizado = normalizar_nombre(nombre_limpio)

                        if nombre_normalizado in ALIAS_TECNICOS:
                            nombre_normalizado = ALIAS_TECNICOS[nombre_normalizado]

                        tecnico_obj = None
                        contratista_obj = None

                        if nombre_normalizado:
                            tecnico_obj = tecnicos_db.get(nombre_normalizado)

                            if not tecnico_obj and nombre_limpio:
                                tecnico_obj = Tecnico.objects.filter(nombre=nombre_limpio).first()
                                if tecnico_obj:
                                    tecnicos_db[nombre_normalizado] = tecnico_obj

                            if not tecnico_obj and nombre_limpio:
                                texto_original = str(nombre_original or "").upper().strip()
                                es_contratista = False

                                if texto_original.startswith("T. EXTERNO") or texto_original.startswith("T EXTERNO"):
                                    es_contratista = True

                                if nombre_normalizado in contratistas_db:
                                    es_contratista = True

                                if nombre_normalizado in CONTRATISTAS_MANUALES:
                                    es_contratista = True

                                contratista_obj = contratistas_db.get(nombre_normalizado) if es_contratista else None

                                tecnico_obj, creado = Tecnico.objects.get_or_create(
                                    nombre=nombre_limpio,
                                    defaults={
                                        "tipo": "contratista" if es_contratista else "interno",
                                        "categoria": "principal" if es_contratista else "remoto",
                                        "contratista": contratista_obj,
                                    }
                                )

                                if not creado:
                                    cambios = []

                                    if es_contratista and tecnico_obj.tipo != "contratista":
                                        tecnico_obj.tipo = "contratista"
                                        cambios.append("tipo")

                                    if es_contratista and contratista_obj and tecnico_obj.contratista_id != contratista_obj.id:
                                        tecnico_obj.contratista = contratista_obj
                                        cambios.append("contratista")

                                    if cambios:
                                        tecnico_obj.save(update_fields=cambios)

                                tecnicos_db[nombre_normalizado] = tecnico_obj

                            if tecnico_obj and tecnico_obj.tipo == "contratista":
                                contratista_obj = tecnico_obj.contratista

                                if not contratista_obj:
                                    contratista_obj = contratistas_db.get(nombre_normalizado)
                                    if contratista_obj:
                                        tecnico_obj.contratista = contratista_obj
                                        tecnico_obj.save(update_fields=["contratista"])

                        valor_pago = row.get("Valor pago técnico")
                        valor_general = row.get("Valor")
                        costo_mano_obra = row.get("Costo mano de obra")

                        es_b2b, cuenta_contable = clasificar_b2b(row.get("Cuenta"), cuentas_b2b_set)

                        servicios_a_crear.append(
                            ServicioTecnico(
                                carga=carga,
                                numero=row.get("Número"),
                                fecha_creacion=convertir_fecha(row.get("Fecha de Creación")),
                                fecha_modificacion=convertir_fecha(row.get("Fecha de modificación")),
                                fecha_visita=limpiar_fecha(row.get("fecha_visita_convertida")),
                                fecha_finalizacion=convertir_fecha(row.get("Fecha de finalizacion")),
                                cuenta=row.get("Cuenta"),
                                cuenta_contable=cuenta_contable,
                                es_b2b=es_b2b,
                                telefono=row.get("Teléfono"),
                                tecnico=nombre_limpio,
                                tecnico_obj=tecnico_obj,
                                contratista=contratista_obj,
                                direccion=row.get("Dirección"),
                                provincia_estado=row.get("Provincia-Estado"),
                                localidad=row.get("Localidad"),
                                tipo_servicio=row.get("Tipo de Servicio"),
                                servicio=row.get("Servicio"),
                                observaciones=row.get("Observaciones (Insumos)"),
                                estado=row.get("Estado"),
                                usuario=row.get("Usuario"),
                                valor=valor_general if pd.notna(valor_general) else None,
                                costo_mano_obra=costo_mano_obra if pd.notna(costo_mano_obra) else None,
                                fecha_pago=convertir_fecha(row.get("Fecha de pago")),
                                valor_pago_original=valor_pago if pd.notna(valor_pago) else 0,
                                valor_pago_tecnico=valor_pago if pd.notna(valor_pago) else 0,
                                tiempo_trabajo_total=row.get("Tiempo de Trabajo Total"),
                                numero_incidencias_dia=int(row.get("numero_incidencias_dia") or 0),
                            )
                        )

                    ServicioTecnico.objects.bulk_create(servicios_a_crear, batch_size=1000)

                mensaje = "Carga procesada correctamente"

            except Exception as e:
                logger.exception("Error al procesar archivos Excel")
                mensaje = f"Error al procesar la carga: {str(e)}"

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

    if not mes or not anio:
        ultima_carga = CargaMensual.objects.order_by("-anio", "-mes").first()
        if ultima_carga:
            mes = str(ultima_carga.mes)
            anio = str(ultima_carga.anio)

    servicios = ServicioTecnico.objects.all().select_related(
        "contratista", "tecnico_obj", "carga"
    ).order_by("-fecha_finalizacion")

    if mes and anio:
        servicios = servicios.filter(carga__mes=mes, carga__anio=anio)
    elif carga_id:
        servicios = servicios.filter(carga_id=carga_id)

    if query:
        servicios = servicios.filter(
            Q(numero__icontains=query) |
            Q(cuenta_contable__icontains=query) |
            Q(cuenta__icontains=query) |
            Q(tecnico__icontains=query) |
            Q(tecnico_obj__nombre__icontains=query) |
            Q(contratista__nombre__icontains=query)
        )

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

    contratistas = Contratista.objects.filter(
        servicios__isnull=False
    ).distinct().order_by("nombre")

    contratista_obj = None
    if contratista_id:
        contratista_obj = Contratista.objects.filter(id=contratista_id).first()

    servicios_qs = ServicioTecnico.objects.filter(
        tecnico_obj__tipo="contratista"
    ).select_related("contratista", "tecnico_obj", "carga")

    if mes and anio:
        servicios_qs = servicios_qs.filter(carga__mes=mes, carga__anio=anio)
    elif carga_id:
        servicios_qs = servicios_qs.filter(carga_id=carga_id)

    if contratista_id:
        servicios_qs = servicios_qs.filter(contratista_id=contratista_id)

    if estado_pago:
        servicios_qs = servicios_qs.filter(estado_pago=estado_pago)

    if tipo_servicio:
        servicios_qs = servicios_qs.filter(tipo_servicio__icontains=tipo_servicio)

    servicios_qs = servicios_qs.order_by("fecha_finalizacion")
    servicios = list(servicios_qs)

    if provincia_estado_sel:
        valor = provincia_estado_sel.strip().upper()
        servicios_filtrados = []

        for s in servicios:
            dato = str(s.provincia_estado).strip().upper() if s.provincia_estado else ""

            if valor == "NAN":
                if dato == "NAN" or dato == "":
                    servicios_filtrados.append(s)
            else:
                if dato == valor:
                    servicios_filtrados.append(s)

        servicios = servicios_filtrados

    provincias_set = set()

    for s in servicios_qs:
        dato = str(s.provincia_estado).strip().upper() if s.provincia_estado else "NAN"
        if not dato:
            dato = "NAN"
        provincias_set.add(dato)

    provincias = sorted(provincias_set)

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

    resumen = obtener_resumen_contratista(servicios)

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
        servicio.save(update_fields=["estado_pago"])
        return JsonResponse({"success": True})

    return JsonResponse({"success": False, "error": "Método no permitido"})


# ==============================
# ACTUALIZAR MONTO
# ==============================

def actualizar_monto(request, servicio_id):
    if request.method == "POST":
        servicio = get_object_or_404(ServicioTecnico, id=servicio_id)
        nuevo_valor = request.POST.get("valor")

        try:
            valor_limpio = str(nuevo_valor).replace(".", "").replace(",", "").strip()
            servicio.valor_pago_tecnico = int(Decimal(valor_limpio))
            servicio.save()
            return JsonResponse({"success": True})
        except (InvalidOperation, TypeError, ValueError):
            return JsonResponse({"success": False})

    return JsonResponse({"success": False})


# ==============================
# ELIMINAR CARGA COMPLETA
# ==============================

def eliminar_carga(request, carga_id):
    carga = get_object_or_404(CargaMensual, id=carga_id)
    carga.delete()
    return redirect("inicio")


# ==============================
# EDITAR MONTO TÉCNICO
# ==============================

@csrf_exempt
def editar_monto_tecnico(request, servicio_id):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            valor_raw = str(data.get("valor_pago_tecnico", "0")).replace(".", "").replace(",", "").strip()
            nuevo_valor = int(Decimal(valor_raw or 0))

            servicio = ServicioTecnico.objects.get(id=servicio_id)

            if servicio.valor_pago_original is None:
                servicio.valor_pago_original = servicio.valor_pago_tecnico

            servicio.valor_pago_tecnico = nuevo_valor
            servicio.save(update_fields=["valor_pago_tecnico", "valor_pago_original"])

            return JsonResponse({"success": True})

        except Exception as e:
            return JsonResponse({
                "success": False,
                "error": str(e)
            })

    return JsonResponse({
        "success": False,
        "error": "Método no permitido"
    })


# ==============================
# PDF CONTRATISTA
# ==============================

def contratista_pdf(request):
    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    carga_id = request.GET.get("carga")
    contratista_id = request.GET.get("contratista_id")
    estado_pago = request.GET.get("estado_pago")
    tipo_servicio = request.GET.get("tipo_servicio")
    provincia_estado_sel = request.GET.get("provincia_estado")

    contratista_obj = None
    if contratista_id:
        contratista_obj = Contratista.objects.filter(id=contratista_id).first()

    servicios_qs = ServicioTecnico.objects.filter(
        Q(tecnico_obj__tipo="contratista") |
        Q(tecnico_obj__isnull=True, contratista__isnull=False)
    ).select_related("contratista", "tecnico_obj", "carga")

    if mes and anio:
        servicios_qs = servicios_qs.filter(carga__mes=mes, carga__anio=anio)
    elif carga_id:
        servicios_qs = servicios_qs.filter(carga_id=carga_id)

    if contratista_id:
        servicios_qs = servicios_qs.filter(contratista_id=contratista_id)

    if estado_pago:
        servicios_qs = servicios_qs.filter(estado_pago=estado_pago)

    if tipo_servicio:
        servicios_qs = servicios_qs.filter(tipo_servicio__icontains=tipo_servicio)

    servicios_qs = servicios_qs.order_by("fecha_finalizacion")
    servicios = list(servicios_qs)

    if provincia_estado_sel:
        valor = provincia_estado_sel.strip().upper()
        servicios_filtrados = []

        for s in servicios:
            dato = str(s.provincia_estado).strip().upper() if s.provincia_estado else ""

            if valor == "NAN":
                if dato == "NAN" or dato == "":
                    servicios_filtrados.append(s)
            else:
                if dato == valor:
                    servicios_filtrados.append(s)

        servicios = servicios_filtrados

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

    resumen = obtener_resumen_contratista(servicios)

    html_string = render_to_string(
        "servicios/contratista_pdf.html",
        {
            "contratista": contratista_obj,
            "servicios": servicios,
            "resumen": resumen,
            "estado_pago": estado_pago,
            "mes": mes,
            "anio": anio,
            "tipo_servicio": tipo_servicio,
            "provincia_estado": provincia_estado_sel,
            "contratista_id": contratista_id,
        }
    )

    buffer = BytesIO()
    pisa_status = pisa.CreatePDF(html_string, dest=buffer)
    buffer.seek(0)

    if pisa_status.err:
        return HttpResponse("Error al generar PDF", status=500)

    response = HttpResponse(buffer.getvalue(), content_type="application/pdf")
    response["Content-Disposition"] = 'inline; filename="reporte_contratista.pdf"'
    return response


# ==============================
# EXPORTAR EXCEL
# ==============================

def exportar_excel(request):
    servicios = ServicioTecnico.objects.all().values()
    df = pd.DataFrame(servicios)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                try:
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
                except Exception:
                    pass

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = "attachment; filename=servicios.xlsx"

    df.to_excel(response, index=False)
    return response
import json
import logging
import re
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO
from django.views.decorators.http import require_POST


import pandas as pd
from django.contrib import messages
from django.db import transaction
from django.db.models import Count, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods
from xhtml2pdf import pisa

from .models import (
    AliasContratista,
    AliasTecnico,
    ArchivoCarga,
    CargaMensual,
    CECO,
    Contratista,
    CuentaB2B,
    ObservacionImportacion,
    ServicioTecnico,
    ServicioTecnicoRaw,
    Tecnico,
)

logger = logging.getLogger(__name__)


# =========================================================
# HELPERS GENERALES
# =========================================================

COLUMNAS_ESPERADAS = [
    "Número",
    "Fecha de Creación",
    "Fecha de modificación",
    "Fecha de la visita",
    "Fecha de finalizacion",
    "Cuenta",
    "Teléfono",
    "Tecnico",
    "Dirección",
    "Provincia-Estado",
    "Localidad",
    "Tipo de Servicio",
    "Servicio",
    "Observaciones (Insumos)",
    "Estado",
    "Usuario",
    "Valor",
    "Costo mano de obra",
    "Fecha de pago",
    "Valor pago técnico",
    "Tiempo de Trabajo Total",
]


def recalcular_estado_carga(archivo):
    """
    Recalcula el estado de la carga en base a observaciones pendientes reales.
    """

    tipos_revision_real = ["contratista_no_encontrado", "tecnico_sin_match"]

    observaciones_pendientes = ObservacionImportacion.objects.filter(
        raw__archivo_carga=archivo,
        tipo__in=tipos_revision_real,
        estado="pendiente"
    ).count()

    archivo.filas_con_observacion = observaciones_pendientes

    if observaciones_pendientes > 0:
        archivo.estado = "procesado_con_observaciones"
    else:
        archivo.estado = "procesado"

    archivo.save(update_fields=[
        "filas_con_observacion",
        "estado",
        "fecha_actualizacion",
    ])


def truncar_texto(valor, largo):
    if valor is None:
        return None
    return str(valor).strip()[:largo]


def quitar_tildes(texto):
    if not texto:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def normalizar_texto_base(valor):
    if not valor:
        return ""
    valor = quitar_tildes(valor)
    valor = valor.upper().strip()
    valor = valor.replace(".", "")
    valor = re.sub(r"\s+", " ", valor)
    return valor.strip()


def normalizar_nombre_persona(valor):
    valor = normalizar_texto_base(valor)

    if not valor:
        return ""

    valor = re.sub(r"^T\s*INTERNO\s*-?\s*", "", valor)
    valor = re.sub(r"^T\s*EXTERNO\s*-?\s*", "", valor)

    valor = re.sub(r"\bEIRL\b", "", valor)
    valor = re.sub(r"\bSPA\b", "", valor)
    valor = re.sub(r"\bLTDA\b", "", valor)
    valor = re.sub(r"\bLIMITADA\b", "", valor)

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


def detectar_tipo_tecnico(texto_original):
    if not texto_original:
        return None

    texto = str(texto_original).upper()

    if "T. EXTERNO" in texto or "T EXTERNO" in texto:
        return "contratista"

    if "T. INTERNO" in texto or "T INTERNO" in texto:
        return "interno"

    return None


def normalizar_empresa(valor):
    valor = normalizar_texto_base(valor)
    if not valor:
        return ""

    valor = re.sub(r"[^A-Z0-9 ]", "", valor)
    valor = re.sub(r"\s+", " ", valor)
    return valor.strip()


def normalizar_rut(valor):
    if not valor:
        return ""

    valor = str(valor).strip().upper()
    valor = valor.replace(".", "").replace(" ", "")
    valor = re.sub(r"[^0-9K\-]", "", valor)

    if "-" not in valor and len(valor) >= 2:
        valor = f"{valor[:-1]}-{valor[-1]}"

    return valor.strip()


def normalizar_cuenta(valor):
    if valor is None:
        return ""

    valor = str(valor).strip().upper()

    if valor in ["", "NAN", "NONE", "NULL"]:
        return ""

    valor = re.sub(r"\s+", "", valor)

    match_fir = re.match(r"^FIR[-_]?([A-Z0-9]+)$", valor)
    if match_fir:
        return f"FIR-{match_fir.group(1)}"

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

    match = re.search(r"FIR\s*-?\s*[A-Z0-9]+", texto)
    if match:
        return normalizar_cuenta(match.group(0))

    match = re.search(r"NEW\s*-?\s*SF[_-]?\s*[A-Z0-9]+", texto)
    if match:
        return normalizar_cuenta(match.group(0))

    return ""


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

        # limpiar fecha basura común
        if fecha.year <= 1900:
            return None

        if timezone.is_naive(fecha):
            fecha = timezone.make_aware(fecha)

        return fecha

    except Exception:
        return None


def limpiar_entero_monetario(valor):
    if valor is None:
        return None

    try:
        if pd.isna(valor):
            return None
    except Exception:
        pass

    texto = str(valor).strip()

    if texto.upper() in ["", "NAN", "NONE", "NULL"]:
        return None

    texto = texto.replace("$", "").replace(" ", "")

    # casos tipo 21.000.00 -> 21000
    if texto.count(".") >= 2 and texto.endswith(".00"):
        texto = texto[:-3].replace(".", "")

    # casos tipo 25.000
    elif texto.count(".") >= 1 and "," not in texto:
        partes = texto.split(".")
        if len(partes[-1]) == 3:
            texto = "".join(partes)

    texto = texto.replace(",", "")

    try:
        return int(Decimal(texto))
    except (InvalidOperation, ValueError):
        return None


def limpiar_decimal(valor):
    entero = limpiar_entero_monetario(valor)
    if entero is None:
        return None
    return Decimal(entero)


def leer_archivo_excel_generico(archivo):
    nombre = archivo.name.lower()

    if nombre.endswith(".xls"):
        tablas = pd.read_html(archivo)
        if not tablas:
            raise ValueError("No se encontraron tablas en el archivo .xls")
        df = tablas[0]
    else:
        df = pd.read_excel(archivo)

    df.columns = [str(c).strip() for c in df.columns]
    return df


def obtener_dict_cuentas_b2b():
    cuentas = CuentaB2B.objects.filter(activo=True)
    return {c.cuenta_normalizada: c for c in cuentas}


def clasificar_b2b_desde_cuenta(cuenta_texto, cuentas_b2b_dict):
    cuenta_contable = extraer_cuenta_contable(cuenta_texto)
    cuenta_normalizada = normalizar_cuenta(cuenta_contable)
    es_b2b = cuenta_normalizada in cuentas_b2b_dict
    return es_b2b, cuenta_normalizada


def crear_observacion(raw, tipo, detalle, valor_detectado=None, sugerencia=None):
    return ObservacionImportacion.objects.create(
        raw=raw,
        tipo=tipo,
        detalle=detalle,
        valor_detectado=valor_detectado,
        sugerencia=sugerencia,
    )


# =========================================================
# MATCHING DE ENTIDADES
# =========================================================

def buscar_tecnico_por_nombre(nombre_limpio):
    if not nombre_limpio:
        return None

    normalizado = normalizar_nombre_persona(nombre_limpio)

    alias = AliasTecnico.objects.filter(
        alias_normalizado=normalizado,
        activo=True
    ).select_related("tecnico").first()

    if alias:
        return alias.tecnico

    return Tecnico.objects.filter(
        nombre_normalizado=normalizado,
        activo=True
    ).first()


def buscar_contratista_por_nombre(nombre_limpio):
    if not nombre_limpio:
        return None

    normalizado = normalizar_nombre_persona(nombre_limpio)

    alias = AliasContratista.objects.filter(
        alias_normalizado=normalizado,
        activo=True
    ).select_related("contratista").first()

    if alias:
        return alias.contratista

    exactos = Contratista.objects.filter(
        nombre_normalizado=normalizado,
        activo=True
    )

    if exactos.count() == 1:
        return exactos.first()

    return None


def resolver_tecnico_y_contratista(nombre_tecnico_original, raw):
    """
    Resuelve técnico y contratista con lógica segura.
    No crea contratista automáticamente.
    """
    if not nombre_tecnico_original or str(nombre_tecnico_original).strip() == "":
        return None, None, "sin_tecnico"

    tipo_detectado = detectar_tipo_tecnico(nombre_tecnico_original)
    nombre_limpio = limpiar_nombre_tecnico(nombre_tecnico_original)
    nombre_normalizado = normalizar_nombre_persona(nombre_limpio)

    tecnico = buscar_tecnico_por_nombre(nombre_limpio)
    contratista = None

    if tecnico:
        if tecnico.tipo == "contratista":
            contratista = tecnico.contratista
        return tecnico, contratista, tipo_detectado

    # si no existe técnico, intentar resolver contratista por nombre
    if tipo_detectado == "contratista":
        contratista = buscar_contratista_por_nombre(nombre_limpio)

        if contratista:
            tecnico = Tecnico.objects.create(
                nombre=nombre_limpio,
                tipo="contratista",
                categoria="remoto",
                contratista=contratista,
                activo=True,
                requiere_revision=False,
            )

            AliasTecnico.objects.get_or_create(
                alias=nombre_limpio,
                defaults={
                    "alias_normalizado": nombre_normalizado,
                    "tecnico": tecnico,
                    "activo": True,
                }
            )

            return tecnico, contratista, tipo_detectado

        # esto sí queda en revisión
        tecnico = Tecnico.objects.create(
            nombre=nombre_limpio,
            tipo="contratista",
            categoria="remoto",
            contratista=None,
            activo=True,
            requiere_revision=True,
        )

        crear_observacion(
            raw,
            "contratista_no_encontrado",
            "No se pudo vincular el técnico externo a un contratista existente.",
            valor_detectado=nombre_tecnico_original,
            sugerencia=nombre_limpio,
        )

        return tecnico, None, tipo_detectado

    # interno o no detectado: crear técnico normal
    tecnico = Tecnico.objects.create(
        nombre=nombre_limpio,
        tipo="interno" if tipo_detectado != "contratista" else "contratista",
        categoria="remoto",
        activo=True,
        requiere_revision=False,
    )

    AliasTecnico.objects.get_or_create(
        alias=nombre_limpio,
        defaults={
            "alias_normalizado": nombre_normalizado,
            "tecnico": tecnico,
            "activo": True,
        }
    )

    return tecnico, None, tipo_detectado


def buscar_ceco_por_cuenta(cuenta_contable):
    if not cuenta_contable:
        return None
    return CECO.objects.filter(
        cuenta_normalizada=cuenta_contable,
        activo=True
    ).first()


# =========================================================
# PROCESAMIENTO RAW -> FINAL
# =========================================================

def guardar_dataframe_en_raw(df, archivo_carga):
    filas = []

    for idx, row in df.iterrows():
        data = {}
        for col in df.columns:
            valor = row[col]
            if isinstance(valor, pd.Timestamp):
                valor = valor.isoformat()
            elif pd.isna(valor):
                valor = None
            else:
                valor = str(valor).strip() if not isinstance(valor, (int, float)) else valor

            data[str(col).strip()] = valor

        numero_ot = None
        try:
            numero_ot = int(float(row.get("Número"))) if row.get("Número") not in [None, ""] else None
        except Exception:
            pass

        tecnico_texto = row.get("Tecnico")
        cuenta_texto = row.get("Cuenta")

        filas.append(
            ServicioTecnicoRaw(
                archivo_carga=archivo_carga,
                fila_numero=idx + 1,
                data=data,
                numero_ot=numero_ot,
                tecnico_texto=truncar_texto(tecnico_texto, 200) if tecnico_texto else None,
                tecnico_normalizado=normalizar_nombre_persona(limpiar_nombre_tecnico(tecnico_texto)) if tecnico_texto else "",
                cuenta_texto=truncar_texto(cuenta_texto, 400) if cuenta_texto else None,
                cuenta_contable=extraer_cuenta_contable(cuenta_texto),
                tipo_servicio=truncar_texto(row.get("Tipo de Servicio"), 150),
                fecha_visita=convertir_fecha(row.get("Fecha de la visita")),
                fecha_finalizacion=convertir_fecha(row.get("Fecha de finalizacion")),
                valor_pago_original_texto=str(row.get("Valor pago técnico")).strip() if row.get("Valor pago técnico") is not None else None,
                valor_pago_tecnico=limpiar_entero_monetario(row.get("Valor pago técnico")),
            )
        )

    ServicioTecnicoRaw.objects.bulk_create(filas, batch_size=500)
    archivo_carga.total_filas = len(filas)
    archivo_carga.save(update_fields=["total_filas", "fecha_actualizacion"])


def procesar_raw_a_servicio(raw, cuentas_b2b_dict):
    data = raw.data or {}

    numero = raw.numero_ot
    fecha_creacion_origen = convertir_fecha(data.get("Fecha de Creación"))
    fecha_modificacion = convertir_fecha(data.get("Fecha de modificación"))
    fecha_visita = convertir_fecha(data.get("Fecha de la visita"))
    fecha_finalizacion = convertir_fecha(data.get("Fecha de finalizacion"))
    fecha_pago = convertir_fecha(data.get("Fecha de pago"))

    cuenta = truncar_texto(data.get("Cuenta"), 400)
    telefono = truncar_texto(data.get("Teléfono"), 100)
    tecnico_origen = data.get("Tecnico")
    tecnico_texto = truncar_texto(tecnico_origen, 150)

    if not tecnico_texto or str(tecnico_texto).strip() == "":
        tecnico_texto = "Sin técnico"
    direccion = truncar_texto(data.get("Dirección"), 300)
    provincia_estado = truncar_texto(data.get("Provincia-Estado"), 150)
    localidad = truncar_texto(data.get("Localidad"), 150)
    tipo_servicio = truncar_texto(data.get("Tipo de Servicio"), 150)
    servicio = truncar_texto(data.get("Servicio"), 300)
    observaciones = data.get("Observaciones (Insumos)")
    estado = truncar_texto(data.get("Estado"), 100)
    usuario = truncar_texto(data.get("Usuario"), 150)
    tiempo_trabajo_total = truncar_texto(data.get("Tiempo de Trabajo Total"), 100)

    valor = limpiar_decimal(data.get("Valor"))
    costo_mano_obra = limpiar_decimal(data.get("Costo mano de obra"))
    valor_pago_tecnico = limpiar_entero_monetario(data.get("Valor pago técnico"))
    valor_pago_original = valor_pago_tecnico

    es_b2b, cuenta_contable = clasificar_b2b_desde_cuenta(cuenta, cuentas_b2b_dict)
    ceco = buscar_ceco_por_cuenta(cuenta_contable)

    tecnico_obj, contratista, tipo_detectado = resolver_tecnico_y_contratista(tecnico_texto, raw)

    requiere_revision = False

    if not fecha_finalizacion and data.get("Fecha de finalizacion"):
        crear_observacion(
            raw,
            "fecha_invalida",
            "No se pudo convertir la fecha de finalización.",
            valor_detectado=str(data.get("Fecha de finalizacion")),
        )
        requiere_revision = True

    if valor_pago_tecnico is None and data.get("Valor pago técnico") not in [None, "", "0", "0.00"]:
        crear_observacion(
            raw,
            "monto_invalido",
            "No se pudo convertir el valor pago técnico.",
            valor_detectado=str(data.get("Valor pago técnico")),
        )
        requiere_revision = True

    if not cuenta_contable and cuenta:
        crear_observacion(
            raw,
            "cuenta_invalida",
            "No se pudo extraer una cuenta contable válida.",
            valor_detectado=cuenta,
        )
        requiere_revision = True

    if tecnico_obj and tecnico_obj.requiere_revision:
        requiere_revision = True

    servicio_existente = ServicioTecnico.objects.filter(raw=raw).first()

    if servicio_existente:
        estado_pago_actual = servicio_existente.estado_pago
        servicio_existente.delete()
    else:
        estado_pago_actual = "aprobado"

    servicio_final = ServicioTecnico.objects.create(
        carga=raw.archivo_carga.carga_mensual,
        archivo_carga=raw.archivo_carga,
        raw=raw,
        contratista=contratista,
        numero=numero,
        fecha_creacion_origen=fecha_creacion_origen,
        fecha_modificacion=fecha_modificacion,
        fecha_visita=fecha_visita,
        fecha_finalizacion=fecha_finalizacion,
        cuenta=cuenta,
        telefono=telefono,
        tecnico=tecnico_texto,
        tecnico_obj=tecnico_obj,
        direccion=direccion,
        provincia_estado=provincia_estado,
        localidad=localidad,
        tipo_servicio=tipo_servicio,
        servicio=servicio,
        observaciones=observaciones,
        estado=estado,
        usuario=usuario,
        valor=valor,
        costo_mano_obra=costo_mano_obra,
        fecha_pago=fecha_pago,
        valor_pago_original=valor_pago_original,
        valor_pago_tecnico=valor_pago_tecnico,
        tiempo_trabajo_total=tiempo_trabajo_total,
        cuenta_contable=cuenta_contable,
        ceco=ceco,
        estado_pago=estado_pago_actual,
        es_b2b=es_b2b,
        requiere_revision=requiere_revision,
    )

    raw.estado = "revision" if requiere_revision else "publicado"
    raw.procesado = True
    raw.publicado = not requiere_revision
    raw.requiere_revision = requiere_revision
    raw.error = None
    raw.save(update_fields=[
        "estado", "procesado", "publicado", "requiere_revision", "error", "fecha_actualizacion"
    ])

    return servicio_final


def recalcular_incidencias_para_archivo(archivo_carga):
    servicios = ServicioTecnico.objects.filter(
        archivo_carga=archivo_carga
    ).exclude(
        fecha_finalizacion__isnull=True
    )

    # Solo servicios con técnico asignado real
    servicios_validos = servicios.exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).exclude(
        tecnico="Sin técnico"
    )

    # =========================
    # Incidencias por día
    # =========================
    agrupados = {}
    for s in servicios_validos:
        clave = (
            s.cuenta_contable or "",
            s.fecha_finalizacion.date() if s.fecha_finalizacion else None
        )
        agrupados[clave] = agrupados.get(clave, 0) + 1

    for s in servicios:
        if not s.tecnico or str(s.tecnico).strip() in ["", "Sin técnico"]:
            s.numero_incidencias_dia = 0
        else:
            clave = (
                s.cuenta_contable or "",
                s.fecha_finalizacion.date() if s.fecha_finalizacion else None
            )
            s.numero_incidencias_dia = agrupados.get(clave, 0)

    ServicioTecnico.objects.bulk_update(servicios, ["numero_incidencias_dia"], batch_size=500)

    # =========================
    # Incidencias 60 días
    # =========================
    servicios_lista = list(servicios)
    for s in servicios_lista:
        if (
            not s.fecha_finalizacion
            or not s.cuenta_contable
            or not s.tecnico
            or str(s.tecnico).strip() in ["", "Sin técnico"]
        ):
            s.numero_incidencias_60_dias = 0
            continue

        fecha_desde = s.fecha_finalizacion - timedelta(days=60)

        cantidad = ServicioTecnico.objects.filter(
            cuenta_contable=s.cuenta_contable,
            fecha_finalizacion__gte=fecha_desde,
            fecha_finalizacion__lte=s.fecha_finalizacion,
        ).exclude(
            tecnico__isnull=True
        ).exclude(
            tecnico=""
        ).exclude(
            tecnico="Sin técnico"
        ).count()

        s.numero_incidencias_60_dias = cantidad

    ServicioTecnico.objects.bulk_update(servicios_lista, ["numero_incidencias_60_dias"], batch_size=200)


def procesar_archivo_carga(archivo_carga):
    archivo_carga.estado = "procesando"
    archivo_carga.mensaje = None
    archivo_carga.save(update_fields=["estado", "mensaje", "fecha_actualizacion"])

    cuentas_b2b_dict = obtener_dict_cuentas_b2b()

    raws = ServicioTecnicoRaw.objects.filter(archivo_carga=archivo_carga, procesado=False).order_by("fila_numero")

    procesadas = 0
    publicadas = 0
    observadas = 0

    for raw in raws:
        try:
            servicio = procesar_raw_a_servicio(raw, cuentas_b2b_dict)
            procesadas += 1

            if servicio:
                if servicio.requiere_revision:
                    observadas += 1
                else:
                    publicadas += 1
            else:
                observadas += 1

        except Exception as e:
            logger.exception("Error procesando raw %s", raw.id)
            raw.estado = "error"
            raw.procesado = True
            raw.error = str(e)
            raw.requiere_revision = True
            raw.save(update_fields=["estado", "procesado", "error", "requiere_revision", "fecha_actualizacion"])

            crear_observacion(
                raw,
                "otro",
                "Error no controlado durante el procesamiento.",
                valor_detectado=str(e),
            )
            procesadas += 1
            observadas += 1

    recalcular_incidencias_para_archivo(archivo_carga)

    tipos_revision_real = ["contratista_no_encontrado", "tecnico_sin_match", "tecnico_ambiguo", "contratista_ambiguo"]

    observaciones_reales = ObservacionImportacion.objects.filter(
        raw__archivo_carga=archivo_carga,
        tipo__in=tipos_revision_real,
        estado="pendiente"
    ).count()

    archivo_carga.filas_procesadas = procesadas
    archivo_carga.filas_publicadas = publicadas
    archivo_carga.filas_con_observacion = observaciones_reales
    archivo_carga.estado = "procesado_con_observaciones" if observaciones_reales > 0 else "procesado"
    archivo_carga.mensaje = (
        f"Procesadas: {procesadas} | "
        f"Publicadas: {publicadas} | "
        f"Observaciones reales: {observaciones_reales}"
    )

    recalcular_incidencias_para_archivo(archivo_carga)

    tipos_revision_real = ["contratista_no_encontrado", "tecnico_sin_match"]

    observaciones_reales = ObservacionImportacion.objects.filter(
        raw__archivo_carga=archivo_carga,
        tipo__in=tipos_revision_real,
        estado="pendiente"
    ).count()

    archivo_carga.filas_procesadas = procesadas
    archivo_carga.filas_publicadas = publicadas
    archivo_carga.filas_con_observacion = observaciones_reales
    archivo_carga.estado = "procesado_con_observaciones" if observaciones_reales > 0 else "procesado"
    archivo_carga.mensaje = (
        f"Procesadas: {procesadas} | "
        f"Publicadas: {publicadas} | "
        f"Observaciones reales: {observaciones_reales}"
    )
    archivo_carga.save(update_fields=[
        "filas_procesadas",
        "filas_publicadas",
        "filas_con_observacion",
        "estado",
        "mensaje",
        "fecha_actualizacion",
    ])    


# =========================================================
# RESÚMENES
# =========================================================

def obtener_resumen_contratista(servicios):
    servicios = list(servicios)

    # Totales generales por número OT
    numeros_totales = {s.numero for s in servicios if s.numero is not None}

    # Preventivas / Correctivas
    numeros_preventivas = {
        s.numero for s in servicios
        if s.numero is not None and s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    }
    numeros_correctivas = {
        s.numero for s in servicios
        if s.numero is not None and s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    }

    # Estados de pago
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

    # B2B / B2C
    numeros_b2b = {
        s.numero for s in servicios
        if s.numero is not None and s.es_b2b
    }
    numeros_b2c = {
        s.numero for s in servicios
        if s.numero is not None and not s.es_b2b
    }

    # Montos
    monto_total = Decimal(sum(Decimal(s.valor_pago_tecnico or 0) for s in servicios))
    monto_aprobado = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "aprobado"
    ))
    monto_revision = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "revision"
    ))
    monto_rechazado = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.estado_pago == "rechazado"
    ))

    monto_preventivas = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
    ))
    monto_correctivas = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
    ))

    monto_b2b = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if s.es_b2b
    ))
    monto_b2c = Decimal(sum(
        Decimal(s.valor_pago_tecnico or 0)
        for s in servicios if not s.es_b2b
    ))

    iva_19 = monto_total * Decimal("0.19")
    monto_total_con_iva = monto_total + iva_19

    # Resumen para tabla de estados
    resumen_estados = {
        "aprobado": {
            "cantidad": len(numeros_aprobado),
            "preventivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "aprobado"
                and s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
            }),
            "correctivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "aprobado"
                and s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
            }),
            "monto": monto_aprobado,
        },
        "revision": {
            "cantidad": len(numeros_revision),
            "preventivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "revision"
                and s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
            }),
            "correctivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "revision"
                and s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
            }),
            "monto": monto_revision,
        },
        "rechazado": {
            "cantidad": len(numeros_rechazado),
            "preventivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "rechazado"
                and s.tipo_servicio and "PREVENTIV" in s.tipo_servicio.upper()
            }),
            "correctivas": len({
                s.numero for s in servicios
                if s.numero is not None and s.estado_pago == "rechazado"
                and s.tipo_servicio and "CORRECTIV" in s.tipo_servicio.upper()
            }),
            "monto": monto_rechazado,
        },
    }

    return {
        "total_mantenciones": len(numeros_totales),

        "total_preventivas": len(numeros_preventivas),
        "total_correctivas": len(numeros_correctivas),

        "cantidad_aprobado": len(numeros_aprobado),
        "cantidad_revision": len(numeros_revision),
        "cantidad_rechazado": len(numeros_rechazado),

        "total_b2b": len(numeros_b2b),
        "total_b2c": len(numeros_b2c),

        "monto_total": monto_total,
        "monto_aprobado": monto_aprobado,
        "monto_revision": monto_revision,
        "monto_rechazado": monto_rechazado,

        "monto_preventivas": monto_preventivas,
        "monto_correctivas": monto_correctivas,

        "monto_b2b": monto_b2b,
        "monto_b2c": monto_b2c,

        "iva_19": iva_19,
        "monto_total_con_iva": monto_total_con_iva,

        "resumen_estados": resumen_estados,
    }


# =========================================================
# VISTAS DE CARGA Y PROCESAMIENTO
# =========================================================

@require_http_methods(["GET", "POST"])
def subir_excel(request):
    mensaje = None

    if request.method == "POST":
        nombre_carga = request.POST.get("nombre_carga")
        mes = request.POST.get("mes")
        anio = request.POST.get("anio")

        archivo1 = request.FILES.get("archivo1")
        archivo2 = request.FILES.get("archivo2")

        if not all([nombre_carga, mes, anio, archivo1, archivo2]):
            messages.error(request, "Debes completar todos los campos.")
            return redirect("subir_excel")

        try:
            with transaction.atomic():
                carga_mensual = CargaMensual.objects.create(
                    nombre=nombre_carga,
                    mes=int(mes),
                    anio=int(anio),
                    activa=True,
                )

                for archivo in [archivo1, archivo2]:
                    contenido = archivo.read()
                    archivo.seek(0)

                    hash_archivo = ArchivoCarga.calcular_hash_desde_bytes(contenido)

                    if ArchivoCarga.objects.filter(hash_archivo=hash_archivo).exists():
                        messages.warning(request, f"El archivo {archivo.name} ya fue cargado antes y se omitió.")
                        continue

                    df = leer_archivo_excel_generico(archivo)

                    archivo_carga = ArchivoCarga.objects.create(
                        carga_mensual=carga_mensual,
                        nombre_original=archivo.name,
                        hash_archivo=hash_archivo,
                        mes=int(mes),
                        anio=int(anio),
                        estado="cargado",
                    )

                    guardar_dataframe_en_raw(df, archivo_carga)
                    procesar_archivo_carga(archivo_carga)

                messages.success(request, "Archivos cargados y procesados correctamente.")
                return redirect("subir_excel")

        except Exception as e:
            logger.exception("Error al subir archivos")
            mensaje = f"Error al procesar archivos: {str(e)}"
            messages.error(request, mensaje)

    return render(request, "servicios/subir_excel.html", {"mensaje": mensaje})


def cargas_archivos(request):
    archivos = ArchivoCarga.objects.select_related("carga_mensual").order_by("-fecha_creacion")
    return render(request, "servicios/cargas_archivos.html", {"archivos": archivos})


def observaciones_importacion(request):
    observaciones = ObservacionImportacion.objects.select_related(
        "raw",
        "raw__archivo_carga",
        "raw__archivo_carga__carga_mensual"
    ).order_by("estado", "-fecha_creacion")

    estado = request.GET.get("estado")
    archivo_id = request.GET.get("archivo_id")
    detalle = request.GET.get("detalle")

    if estado:
        observaciones = observaciones.filter(estado=estado)

    if archivo_id:
        observaciones = observaciones.filter(raw__archivo_carga_id=archivo_id)

    if detalle:
        observaciones = observaciones.filter(detalle=detalle)

    detalles_disponibles = (
        ObservacionImportacion.objects
        .exclude(detalle__isnull=True)
        .exclude(detalle="")
        .values_list("detalle", flat=True)
        .distinct()
        .order_by("detalle")
    )

    return render(
        request,
        "servicios/observaciones_importacion.html",
        {
            "observaciones": observaciones,
            "archivo_id": archivo_id,
            "estado_sel": estado,
            "detalle_sel": detalle,
            "detalles_disponibles": detalles_disponibles,
        }
    )


# =========================================================
# VISTA CONTRATISTAS
# =========================================================

def contratista(request):
    servicios = ServicioTecnico.objects.select_related(
        "contratista", "tecnico_obj", "ceco", "archivo_carga"
    ).exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).exclude(
        tecnico="Sin técnico"
    ).filter(
        tecnico_obj__tipo="contratista"
    )

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    contratista_id = request.GET.get("contratista_id")
    tipo_servicio = request.GET.get("tipo_servicio")
    provincia_estado = request.GET.get("provincia_estado")
    estado_pago = request.GET.get("estado_pago")

    if mes:
        servicios = servicios.filter(carga__mes=mes)
    if anio:
        servicios = servicios.filter(carga__anio=anio)
    if contratista_id:
        servicios = servicios.filter(contratista_id=contratista_id)
    if tipo_servicio:
        servicios = servicios.filter(tipo_servicio__icontains=tipo_servicio)
    if provincia_estado:
        servicios = servicios.filter(provincia_estado=provincia_estado)
    if estado_pago:
        servicios = servicios.filter(estado_pago=estado_pago)

    contratistas = Contratista.objects.filter(activo=True).order_by("nombre")
    meses_disponibles = CargaMensual.objects.order_by("mes").values_list("mes", flat=True).distinct()
    anios_disponibles = CargaMensual.objects.order_by("anio").values_list("anio", flat=True).distinct()
    provincias = ServicioTecnico.objects.exclude(
        provincia_estado__isnull=True
    ).exclude(
        provincia_estado=""
    ).values_list("provincia_estado", flat=True).distinct().order_by("provincia_estado")

    contratista_obj = None
    if contratista_id:
        contratista_obj = Contratista.objects.filter(id=contratista_id).first()

    resumen = obtener_resumen_contratista(servicios)

    contexto = {
        "servicios": servicios.order_by("-fecha_finalizacion")[:500],
        "resumen": resumen,
        "contratistas": contratistas,
        "contratista": contratista_obj,
        "meses_disponibles": meses_disponibles,
        "anios_disponibles": anios_disponibles,
        "provincia": provincias,
        "mes": mes,
        "anio": anio,
        "contratista_id": contratista_id,
        "tipo_servicio": tipo_servicio,
        "provincia_estado_sel": provincia_estado,
        "estado_pago": estado_pago,
    }
    return render(request, "servicios/contratista.html", contexto)


# =========================================================
# VISTA INTERNOS
# =========================================================

def internos(request):
    servicios = ServicioTecnico.objects.select_related("tecnico_obj").filter(
        tecnico_obj__tipo="interno"
    ).exclude(
        tecnico__isnull=True
    ).exclude(
        tecnico=""
    ).exclude(
        tecnico="Sin técnico"
    )

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    tecnico = request.GET.get("tecnico")
    tipo_servicio = request.GET.get("tipo_servicio")

    if mes:
        servicios = servicios.filter(carga__mes=mes)
    if anio:
        servicios = servicios.filter(carga__anio=anio)
    if tecnico:
        servicios = servicios.filter(tecnico=tecnico)
    if tipo_servicio:
        servicios = servicios.filter(tipo_servicio__icontains=tipo_servicio)

    tecnicos = servicios.values_list(
        "tecnico", flat=True
    ).distinct().order_by("tecnico")

    resumen = {
        "total": servicios.values("numero").distinct().count(),
        "costo_total": servicios.aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
        "preventivos": servicios.filter(tipo_servicio__icontains="PREVENTIV").values("numero").distinct().count(),
        "correctivos": servicios.filter(tipo_servicio__icontains="CORRECTIV").values("numero").distinct().count(),
        "costo_preventivas": servicios.filter(tipo_servicio__icontains="PREVENTIV").aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
        "costo_correctivas": servicios.filter(tipo_servicio__icontains="CORRECTIV").aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
        "total_b2b": servicios.filter(es_b2b=True).values("numero").distinct().count(),
        "total_b2c": servicios.filter(es_b2b=False).values("numero").distinct().count(),
        "costo_b2b": servicios.filter(es_b2b=True).aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
        "costo_b2c": servicios.filter(es_b2b=False).aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
    }

    tabla_tecnicos = []
    tecnicos_resumen = servicios.values("tecnico").annotate(
        total=Count("numero", distinct=True),
        costo_total=Coalesce(Sum("valor_pago_tecnico"), Value(0)),
    ).order_by("tecnico")

    for t in tecnicos_resumen:
        qs_t = servicios.filter(tecnico=t["tecnico"])
        tabla_tecnicos.append({
            "tecnico": t["tecnico"],
            "total": t["total"],
            "b2b": qs_t.filter(es_b2b=True).values("numero").distinct().count(),
            "b2c": qs_t.filter(es_b2b=False).values("numero").distinct().count(),
            "costo_b2b": qs_t.filter(es_b2b=True).aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
            "costo_b2c": qs_t.filter(es_b2b=False).aggregate(x=Coalesce(Sum("valor_pago_tecnico"), Value(0)))["x"],
            "costo_total": t["costo_total"],
        })

    contexto = {
        "servicios": servicios.order_by("-fecha_finalizacion")[:500],
        "resumen": resumen,
        "tabla_servicios": tabla_tecnicos,
        "tabla_tecnicos": tabla_tecnicos,
        "tecnicos": tecnicos,
        "tecnico_seleccionado": tecnico,
        "meses_disponibles": CargaMensual.objects.order_by("mes").values_list("mes", flat=True).distinct(),
        "anios_disponibles": CargaMensual.objects.order_by("anio").values_list("anio", flat=True).distinct(),
        "mes": mes,
        "anio": anio,
        "tipo_servicio": tipo_servicio,
    }
    return render(request, "servicios/internos.html", contexto)


# =========================================================
# BUSCADOR
# =========================================================

def buscador_servicios(request):
    servicios = ServicioTecnico.objects.select_related("contratista", "tecnico_obj", "ceco")

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    contratista_id = request.GET.get("contratista_id")
    tipo_servicio = request.GET.get("tipo_servicio")
    provincia_estado = request.GET.get("provincia_estado")
    tecnico_sel = request.GET.get("tecnico")
    query = request.GET.get("q")

    if mes:
        servicios = servicios.filter(carga__mes=mes)
    if anio:
        servicios = servicios.filter(carga__anio=anio)
    if contratista_id:
        servicios = servicios.filter(contratista_id=contratista_id)
    if tipo_servicio:
        servicios = servicios.filter(tipo_servicio__icontains=tipo_servicio)
    if provincia_estado:
        servicios = servicios.filter(provincia_estado=provincia_estado)
    if tecnico_sel:
        servicios = servicios.filter(tecnico=tecnico_sel)
    if query:
        servicios = servicios.filter(
            Q(tecnico__icontains=query) |
            Q(cuenta_contable__icontains=query) |
            Q(cuenta__icontains=query) |
            Q(numero__icontains=query)
        )

    contratistas = Contratista.objects.filter(activo=True).order_by("nombre")
    tecnicos = ServicioTecnico.objects.exclude(tecnico__isnull=True).exclude(tecnico="").values_list("tecnico", flat=True).distinct().order_by("tecnico")
    provincias = ServicioTecnico.objects.exclude(provincia_estado__isnull=True).exclude(provincia_estado="").values_list("provincia_estado", flat=True).distinct().order_by("provincia_estado")

    contexto = {
        "servicios": servicios.order_by("-fecha_finalizacion")[:500],
        "contratistas": contratistas,
        "tecnicos": tecnicos,
        "provincia": provincias,
        "meses_disponibles": CargaMensual.objects.order_by("mes").values_list("mes", flat=True).distinct(),
        "anios_disponibles": CargaMensual.objects.order_by("anio").values_list("anio", flat=True).distinct(),
        "mes": mes,
        "anio": anio,
        "contratista_id": contratista_id,
        "tipo_servicio": tipo_servicio,
        "provincia_estado_sel": provincia_estado,
        "tecnico_sel": tecnico_sel,
        "query": query,
    }
    return render(request, "servicios/buscador.html", contexto)


# =========================================================
# ACTUALIZACIÓN DE MONTO / ESTADO
# =========================================================

@require_http_methods(["POST"])
def actualizar_valor_pago(request, servicio_id):
    servicio = get_object_or_404(ServicioTecnico, id=servicio_id)

    try:
        if request.content_type == "application/json":
            data = json.loads(request.body)
            valor = int(data.get("valor_pago_tecnico", 0))
        else:
            valor = int(request.POST.get("valor_pago_tecnico", 0))

        servicio.valor_pago_tecnico = valor
        servicio.save(update_fields=["valor_pago_tecnico", "fecha_actualizacion"])

        return JsonResponse({
            "ok": True,
            "valor_pago_tecnico": valor
        })

    except Exception as e:
        return JsonResponse({
            "ok": False,
            "error": str(e)
        }, status=400)


@require_http_methods(["POST"])
def actualizar_estado_pago(request, servicio_id):
    servicio = get_object_or_404(ServicioTecnico, id=servicio_id)

    nuevo_estado = (request.POST.get("estado_pago") or "").strip().lower()

    estados_validos = {"aprobado", "revision", "rechazado"}

    if nuevo_estado not in estados_validos:
        return JsonResponse({
            "ok": False,
            "error": f"Estado inválido: {nuevo_estado}"
        }, status=400)

    servicio.estado_pago = nuevo_estado
    servicio.save(update_fields=["estado_pago", "fecha_actualizacion"])

    return JsonResponse({
        "ok": True,
        "estado_pago": servicio.estado_pago
    })


# =========================================================
# PDF
# =========================================================

def render_to_pdf(template_src, context_dict):
    from django.template.loader import get_template

    template = get_template(template_src)
    html = template.render(context_dict)
    result = BytesIO()
    pdf = pisa.pisaDocument(BytesIO(html.encode("utf-8")), result)

    if not pdf.err:
        return HttpResponse(result.getvalue(), content_type="application/pdf")
    return None


def contratista_pdf(request):
    servicios = ServicioTecnico.objects.select_related("contratista", "ceco")

    mes = request.GET.get("mes")
    anio = request.GET.get("anio")
    contratista_id = request.GET.get("contratista_id")
    tipo_servicio = request.GET.get("tipo_servicio")
    provincia_estado = request.GET.get("provincia_estado")
    estado_pago = request.GET.get("estado_pago")
    columnas = request.GET.getlist("columnas")

    if mes:
        servicios = servicios.filter(carga__mes=mes)
    if anio:
        servicios = servicios.filter(carga__anio=anio)
    if contratista_id:
        servicios = servicios.filter(contratista_id=contratista_id)
    if tipo_servicio:
        servicios = servicios.filter(tipo_servicio__icontains=tipo_servicio)
    if provincia_estado:
        servicios = servicios.filter(provincia_estado=provincia_estado)
    if estado_pago:
        servicios = servicios.filter(estado_pago=estado_pago)

    contratista = Contratista.objects.filter(id=contratista_id).first() if contratista_id else None
    resumen = obtener_resumen_contratista(servicios)

    meses_map = {
        "1": "Enero", "2": "Febrero", "3": "Marzo", "4": "Abril",
        "5": "Mayo", "6": "Junio", "7": "Julio", "8": "Agosto",
        "9": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
    }
    cantidad_columnas = len(columnas)

    for s in servicios:
        s.tecnico_pdf = s.tecnico_obj.nombre if getattr(s, "tecnico_obj", None) else (s.tecnico or "-")

    context = {
        "servicios": servicios.order_by("-fecha_finalizacion"),
        "contratista": contratista,
        "resumen": resumen,
        "columnas": columnas,
        "mes_mostrar": meses_map.get(str(mes), "Todos"),
        "anio_mostrar": anio or "Todos",
        "estado_pago_mostrar": estado_pago or "Todos",
        "tipo_servicio_mostrar": tipo_servicio or "Todos",
        "provincia_estado_mostrar": provincia_estado or "Todas",
        "cantidad_columnas": cantidad_columnas,
    }

    pdf = render_to_pdf("servicios/contratista_pdf.html", context)
    return pdf if pdf else HttpResponse("Error al generar PDF", status=400)


# =========================================================
# PLACEHOLDER EXPORTAR EXCEL
# =========================================================

def exportar_excel(request):
    return HttpResponse("Pendiente implementar exportación Excel profesional.")





# =========================================================
# RESOLUCIÓN DE OBSERVACIONES
# =========================================================

def detalle_observacion(request, observacion_id):
    observacion = get_object_or_404(
        ObservacionImportacion.objects.select_related(
            "raw",
            "raw__archivo_carga",
            "raw__archivo_carga__carga_mensual",
        ),
        id=observacion_id
    )

    contratistas = Contratista.objects.filter(activo=True).order_by("nombre")
    tecnicos = Tecnico.objects.filter(activo=True).order_by("nombre")

    contexto = {
        "observacion": observacion,
        "raw": observacion.raw,
        "contratistas": contratistas,
        "tecnicos": tecnicos,
    }
    return render(request, "servicios/detalle_observacion.html", contexto)


@require_POST
def marcar_observacion_ignorada(request, observacion_id):
    observacion = get_object_or_404(ObservacionImportacion, id=observacion_id)

    observacion.estado = "ignorada"
    observacion.resuelto_por = "sistema"  # si después agregas auth, aquí va request.user.username
    observacion.fecha_resolucion = timezone.now()
    observacion.comentario_resolucion = request.POST.get("comentario", "Marcada como ignorada")
    observacion.save(update_fields=[
        "estado",
        "resuelto_por",
        "fecha_resolucion",
        "comentario_resolucion",
        "fecha_actualizacion",
    ])

    messages.warning(request, "Observación marcada como ignorada.")
    recalcular_estado_carga(observacion.raw.archivo_carga)
    return redirect("detalle_observacion", observacion_id=observacion.id)


@require_POST
def resolver_observacion_contratista(request, observacion_id):
    observacion = get_object_or_404(
        ObservacionImportacion.objects.select_related("raw"),
        id=observacion_id
    )
    raw = observacion.raw

    contratista_id = request.POST.get("contratista_id")
    crear_alias = request.POST.get("crear_alias") == "on"

    if not contratista_id:
        messages.error(request, "Debes seleccionar un contratista.")
        return redirect("detalle_observacion", observacion_id=observacion.id)

    contratista = get_object_or_404(Contratista, id=contratista_id, activo=True)

    nombre_tecnico_original = raw.tecnico_texto or raw.data.get("Tecnico")
    nombre_limpio = limpiar_nombre_tecnico(nombre_tecnico_original)
    nombre_normalizado = normalizar_nombre_persona(nombre_limpio)

    tecnico = buscar_tecnico_por_nombre(nombre_limpio)

    if tecnico:
        tecnico.tipo = "contratista"
        tecnico.contratista = contratista
        tecnico.requiere_revision = False
        tecnico.save(update_fields=["tipo", "contratista", "requiere_revision", "fecha_actualizacion"])
    else:
        tecnico = Tecnico.objects.create(
            nombre=nombre_limpio,
            tipo="contratista",
            categoria="remoto",
            contratista=contratista,
            activo=True,
            requiere_revision=False,
        )

    if crear_alias and nombre_limpio:
        AliasContratista.objects.get_or_create(
            alias=nombre_limpio,
            defaults={
                "alias_normalizado": nombre_normalizado,
                "contratista": contratista,
                "creado_en_revision": True,
                "activo": True,
            }
        )

    # también conviene guardar alias técnico
    if nombre_limpio:
        AliasTecnico.objects.get_or_create(
            alias=nombre_limpio,
            defaults={
                "alias_normalizado": nombre_normalizado,
                "tecnico": tecnico,
                "activo": True,
            }
        )

    observacion.estado = "resuelta"
    observacion.resuelto_por = "sistema"
    observacion.fecha_resolucion = timezone.now()
    observacion.comentario_resolucion = f"Vinculado a contratista: {contratista.nombre}"
    observacion.save(update_fields=[
        "estado",
        "resuelto_por",
        "fecha_resolucion",
        "comentario_resolucion",
        "fecha_actualizacion",
    ])

    messages.success(request, "Contratista vinculado correctamente. Ahora reprocesa la fila.")
    recalcular_estado_carga(raw.archivo_carga)
    return redirect("detalle_observacion", observacion_id=observacion.id)


@require_POST
def resolver_observacion_tecnico(request, observacion_id):
    observacion = get_object_or_404(
        ObservacionImportacion.objects.select_related("raw"),
        id=observacion_id
    )
    raw = observacion.raw

    tecnico_id = request.POST.get("tecnico_id")
    crear_alias = request.POST.get("crear_alias") == "on"

    if not tecnico_id:
        messages.error(request, "Debes seleccionar un técnico.")
        return redirect("detalle_observacion", observacion_id=observacion.id)

    tecnico = get_object_or_404(Tecnico, id=tecnico_id, activo=True)

    nombre_tecnico_original = raw.tecnico_texto or raw.data.get("Tecnico")
    nombre_limpio = limpiar_nombre_tecnico(nombre_tecnico_original)
    nombre_normalizado = normalizar_nombre_persona(nombre_limpio)

    if crear_alias and nombre_limpio:
        AliasTecnico.objects.get_or_create(
            alias=nombre_limpio,
            defaults={
                "alias_normalizado": nombre_normalizado,
                "tecnico": tecnico,
                "activo": True,
            }
        )

    observacion.estado = "resuelta"
    observacion.resuelto_por = "sistema"
    observacion.fecha_resolucion = timezone.now()
    observacion.comentario_resolucion = f"Vinculado a técnico: {tecnico.nombre}"
    observacion.save(update_fields=[
        "estado",
        "resuelto_por",
        "fecha_resolucion",
        "comentario_resolucion",
        "fecha_actualizacion",
    ])

    messages.success(request, "Técnico vinculado correctamente. Ahora reprocesa la fila.")
    recalcular_estado_carga(raw.archivo_carga)
    return redirect("detalle_observacion", observacion_id=observacion.id)


@require_POST
def reprocesar_raw(request, raw_id):

    raw = get_object_or_404(
        ServicioTecnicoRaw.objects.select_related("archivo_carga", "archivo_carga__carga_mensual"),
        id=raw_id
    )

    # borrar publicación anterior si existía
    ServicioTecnico.objects.filter(raw=raw).delete()

    # limpiar observaciones pendientes o errores anteriores
    raw.estado = "pendiente"
    raw.procesado = False
    raw.publicado = False
    raw.requiere_revision = False
    raw.error = None
    raw.save(update_fields=[
        "estado",
        "procesado",
        "publicado",
        "requiere_revision",
        "error",
        "fecha_actualizacion",
    ])

    cuentas_b2b_dict = obtener_dict_cuentas_b2b()
    servicio = procesar_raw_a_servicio(raw, cuentas_b2b_dict)

    # recalcular incidencias del archivo
    recalcular_incidencias_para_archivo(raw.archivo_carga)

    # actualizar métricas del archivo
    archivo = raw.archivo_carga
    archivo.filas_procesadas = ServicioTecnicoRaw.objects.filter(
        archivo_carga=archivo,
        procesado=True
    ).count()

    archivo.filas_publicadas = ServicioTecnicoRaw.objects.filter(
        archivo_carga=archivo,
        publicado=True
    ).count()

    tipos_revision_real = ["contratista_no_encontrado", "tecnico_sin_match"]

    archivo.filas_con_observacion = ObservacionImportacion.objects.filter(
        raw__archivo_carga=archivo,
        tipo__in=tipos_revision_real,
        estado="pendiente"
    ).count()

    archivo.estado = "procesado_con_observaciones" if archivo.filas_con_observacion > 0 else "procesado"
    archivo.mensaje = (
        f"Procesadas: {archivo.filas_procesadas} | "
        f"Publicadas: {archivo.filas_publicadas} | "
        f"Observaciones reales: {archivo.filas_con_observacion}"
    )
    archivo.save(update_fields=[
        "filas_procesadas",
        "filas_publicadas",
        "filas_con_observacion",
        "estado",
        "mensaje",
        "fecha_actualizacion",
    ])

    if servicio and not servicio.requiere_revision:
        messages.success(request, "Fila reprocesada y publicada correctamente.")
    else:
        messages.warning(request, "Fila reprocesada, pero sigue con observaciones.")

    # redirige al detalle de la primera observación pendiente si existe
    obs = raw.observaciones.order_by("-fecha_creacion").first()
    if obs:
        return redirect("detalle_observacion", observacion_id=obs.id)
    recalcular_estado_carga(archivo)
    return redirect("observaciones_importacion")





@require_POST
def crear_contratista_desde_observacion(request, observacion_id):
    observacion = get_object_or_404(
        ObservacionImportacion.objects.select_related("raw"),
        id=observacion_id
    )
    raw = observacion.raw

    nombre_detectado = limpiar_nombre_tecnico(raw.tecnico_texto or raw.data.get("Tecnico"))
    if not nombre_detectado:
        messages.error(request, "No se detectó un nombre válido para crear el contratista.")
        return redirect("detalle_observacion", observacion_id=observacion.id)

    nombre_empresa = request.POST.get("nombre_empresa") or nombre_detectado
    rut = request.POST.get("rut")
    correo = request.POST.get("correo")
    ciudad = request.POST.get("ciudad")
    fono = request.POST.get("fono")
    banco = request.POST.get("banco")
    tipo_cuenta = request.POST.get("tipo_cuenta")
    numero_cuenta = request.POST.get("numero_cuenta")
    categoria = request.POST.get("categoria") or "Factura"

    contratista = Contratista.objects.create(
        nombre=nombre_detectado,
        nombre_empresa=nombre_empresa,
        rut=rut or None,
        correo=correo or None,
        ciudad=ciudad or None,
        fono=fono or None,
        banco=banco or None,
        tipo_cuenta=tipo_cuenta or None,
        numero_cuenta=numero_cuenta or None,
        categoria=categoria,
        origen="revision",
        activo=True,
        requiere_revision=False,
    )

    tecnico = buscar_tecnico_por_nombre(nombre_detectado)

    if tecnico:
        tecnico.tipo = "contratista"
        tecnico.contratista = contratista
        tecnico.requiere_revision = False
        tecnico.save(update_fields=["tipo", "contratista", "requiere_revision", "fecha_actualizacion"])
    else:
        tecnico = Tecnico.objects.create(
            nombre=nombre_detectado,
            tipo="contratista",
            categoria="remoto",
            contratista=contratista,
            activo=True,
            requiere_revision=False,
        )

    AliasContratista.objects.get_or_create(
        alias=nombre_detectado,
        defaults={
            "alias_normalizado": normalizar_nombre_persona(nombre_detectado),
            "contratista": contratista,
            "creado_en_revision": True,
            "activo": True,
        }
    )

    AliasTecnico.objects.get_or_create(
        alias=nombre_detectado,
        defaults={
            "alias_normalizado": normalizar_nombre_persona(nombre_detectado),
            "tecnico": tecnico,
            "activo": True,
        }
    )

    observacion.estado = "resuelta"
    observacion.resuelto_por = "sistema"
    observacion.fecha_resolucion = timezone.now()
    observacion.comentario_resolucion = f"Se creó contratista nuevo: {contratista.nombre}"
    observacion.save(update_fields=[
        "estado",
        "resuelto_por",
        "fecha_resolucion",
        "comentario_resolucion",
        "fecha_actualizacion",
    ])

    messages.success(request, "Contratista creado correctamente. Ahora reprocesa la fila.")
    recalcular_estado_carga(observacion.raw.archivo_carga)
    return redirect("detalle_observacion", observacion_id=observacion.id)
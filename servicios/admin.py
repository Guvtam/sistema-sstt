from django.contrib import admin
from .models import (
    CargaMensual,
    Contratista,
    Tecnico,
    CuentaB2B,
    ServicioTecnico,
    CECO,
)


@admin.register(CargaMensual)
class CargaMensualAdmin(admin.ModelAdmin):
    list_display = ("id", "nombre", "mes", "anio", "fecha_carga")
    search_fields = ("nombre",)
    list_filter = ("anio", "mes")


@admin.register(Contratista)
class ContratistaAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "nombre",
        "nombre_empresa",
        "rut",
        "ciudad",
        "categoria",
        "fecha_creacion",
    )
    search_fields = ("nombre", "nombre_empresa", "rut")
    list_filter = ("categoria", "ciudad")


@admin.register(Tecnico)
class TecnicoAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "nombre",
        "tipo",
        "categoria",
        "contratista",
        "fecha_creacion",
    )
    search_fields = ("nombre",)
    list_filter = ("tipo", "categoria", "contratista")
    list_editable = ("tipo", "categoria", "contratista")
    ordering = ("nombre",)


@admin.register(CuentaB2B)
class CuentaB2BAdmin(admin.ModelAdmin):
    list_display = ("id", "cuenta", "nombre", "kam")
    search_fields = ("cuenta", "nombre", "kam")


@admin.register(ServicioTecnico)
class ServicioTecnicoAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "numero",
        "tecnico",
        "tecnico_obj",
        "contratista",
        "cuenta_contable",
        "tipo_servicio",
        "estado_pago",
        "es_b2b",
        "fecha_finalizacion",
    )
    search_fields = (
        "numero",
        "tecnico",
        "cuenta",
        "cuenta_contable",
        "servicio",
    )
    list_filter = (
        "estado_pago",
        "es_b2b",
        "tipo_servicio",
        "fecha_finalizacion",
    )
    autocomplete_fields = ("tecnico_obj", "contratista", "carga")
    ordering = ("-fecha_finalizacion",)


@admin.register(CECO)
class CECOAdmin(admin.ModelAdmin):
    list_display = ("id", "cuenta", "ceco", "nombre", "estado")
    search_fields = ("cuenta", "ceco", "nombre")
    list_filter = ("estado",) 
from django.urls import path
from . import views

urlpatterns = [
    path("", views.contratista, name="contratista"),
    path("subir-excel/", views.subir_excel, name="subir_excel"),
    path("buscador/", views.buscador_servicios, name="buscador_servicios"),
    path("internos/", views.internos, name="internos"),
    path("exportar-excel/", views.exportar_excel, name="exportar_excel"),
    path("contratista/pdf/", views.contratista_pdf, name="contratista_pdf"),

    # nuevas vistas de control de carga
    path("cargas/", views.cargas_archivos, name="cargas_archivos"),
    path("observaciones-importacion/", views.observaciones_importacion, name="observaciones_importacion"),

    # edición ajax
    path("servicio/<int:servicio_id>/actualizar-valor/", views.actualizar_valor_pago, name="actualizar_valor_pago"),
    path("servicio/<int:servicio_id>/actualizar-estado/", views.actualizar_estado_pago, name="actualizar_estado_pago"),

    path("observacion/<int:observacion_id>/", views.detalle_observacion, name="detalle_observacion"),
    path("observacion/<int:observacion_id>/ignorar/", views.marcar_observacion_ignorada, name="marcar_observacion_ignorada"),
    path("observacion/<int:observacion_id>/resolver-contratista/", views.resolver_observacion_contratista, name="resolver_observacion_contratista"),
    path("observacion/<int:observacion_id>/resolver-tecnico/", views.resolver_observacion_tecnico, name="resolver_observacion_tecnico"),
    path("raw/<int:raw_id>/reprocesar/", views.reprocesar_raw, name="reprocesar_raw"),
    # rutas nuevas
    path("servicio/<int:servicio_id>/actualizar-valor/",views.actualizar_valor_pago,name="actualizar_valor_pago"),
    path("servicio/<int:servicio_id>/actualizar-estado/",views.actualizar_estado_pago,name="actualizar_estado_pago"),

# aliases para compatibilidad con templates antiguos
    path("servicio/<int:servicio_id>/editar-valor/",views.actualizar_valor_pago,name="editar_valor_pago"),
    path("servicio/<int:servicio_id>/cambiar-estado/",views.actualizar_estado_pago,name="cambiar_estado_pago"),
    path("servicio/<int:servicio_id>/editar-monto-tecnico/",views.actualizar_valor_pago,name="editar_monto_tecnico"),

    path("observacion/<int:observacion_id>/crear-contratista/",views.crear_contratista_desde_observacion,name="crear_contratista_desde_observacion"),
    
]
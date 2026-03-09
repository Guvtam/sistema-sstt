from django.urls import path
from .views import subir_excel
from . import views

urlpatterns = [
    path('', views.inicio, name="inicio"),
    path("subir_excel/", subir_excel, name="subir_excel"),
    path("buscador/", views.buscador_servicios, name="buscador_servicios"),
    path("contratista/", views.contratista, name="contratista" ),
    path('cambiar-estado/<int:servicio_id>/', views.cambiar_estado_pago, name='cambiar_estado_pago'),
    path("actualizar-monto/<int:servicio_id>/", views.actualizar_monto, name="actualizar_monto"),
    path("internos/", views.internos, name="internos"),
    path('editar_monto_tecnico/<int:servicio_id>/', views.editar_monto_tecnico, name='editar_monto_tecnico'),
]
from django.contrib import admin
from .models import ServicioTecnico, Contratista, CargaMensual

# Register your models here.
admin.site.register(ServicioTecnico)
admin.site.register(Contratista)
admin.site.register(CargaMensual)
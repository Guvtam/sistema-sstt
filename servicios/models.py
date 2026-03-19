from django.db import models


class CargaMensual(models.Model):
    nombre = models.CharField(max_length=150)
    mes = models.IntegerField(null=True, blank=True)
    anio = models.IntegerField(null=True, blank=True)
    fecha_carga = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.nombre
    


    
    
    
class Contratista(models.Model):
    nombre = models.CharField(max_length=150)
    nombre_empresa = models.CharField(max_length=150)
    rut = models.CharField(max_length=12, blank=True, null=True)
    fono = models.CharField(max_length=20, blank=True, null=True)  # Número telefónico
    correo = models.EmailField(blank=True, null=True)
    ciudad = models.CharField(max_length=100, blank=True, null=True)
    
    banco = models.CharField(max_length=50, blank=True, null=True)
    tipo_cuenta = models.CharField(max_length=50, blank=True, null=True)
    numero_cuenta = models.CharField(max_length=30, blank=True, null=True)
    
    categoria = models.CharField(max_length=50, blank=True, null=True)
    
    fecha_creacion = models.DateTimeField(auto_now_add=True)  # Para saber cuándo se agregó

    def __str__(self):
        return f"{self.nombre} - {self.nombre_empresa}"
    

class Tecnico(models.Model):
    nombre = models.CharField(max_length=150, unique=True)
    
    TIPO_CHOICES = [
        ("interno", "Técnico Interno"),
        ("contratista", "Técnico Contratista"),
    ]
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default="interno")
    
    CATEGORIA_CHOICES = [
        ("principal", "Principal"),
        ("remoto", "Remoto"),
    ]
    categoria = models.CharField(max_length=20, choices=CATEGORIA_CHOICES, default="remoto")
    
    contratista = models.ForeignKey(
        Contratista,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="tecnicos"
    )
    
    fecha_creacion = models.DateTimeField(auto_now_add=True)

    @property
    def tecnico_nombre(self):
        """Obtiene el nombre del técnico de forma segura"""
        if self.tecnico_obj:
            return self.tecnico_obj.nombre
        return self.tecnico  # Fallback al campo legacy
    
    def __str__(self):
        return f"{self.nombre} ({self.tipo}/{self.categoria})"


class ServicioTecnico(models.Model):

    carga = models.ForeignKey(
        CargaMensual,
        on_delete=models.CASCADE,
        related_name="servicios"
    )

    contratista = models.ForeignKey(
        Contratista,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios"
    )

    numero = models.IntegerField(null=True, blank=True)
    fecha_creacion = models.DateTimeField(null=True, blank=True)
    fecha_modificacion = models.DateTimeField(null=True, blank=True)
    fecha_visita = models.DateTimeField(null=True, blank=True)
    fecha_finalizacion = models.DateTimeField(null=True, blank=True)

    cuenta = models.CharField(max_length=150, null=True, blank=True)
    telefono = models.CharField(max_length=50, null=True, blank=True)
    tecnico = models.CharField(max_length=150, null=True, blank=True)  # Legacy, para transición
    tecnico_obj = models.ForeignKey(
        Tecnico,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios"
    )
    direccion = models.CharField(max_length=255, null=True, blank=True)
    provincia_estado = models.CharField(max_length=150, null=True, blank=True)
    localidad = models.CharField(max_length=150, null=True, blank=True)

    tipo_servicio = models.CharField(max_length=150, null=True, blank=True)
    servicio = models.CharField(max_length=150, null=True, blank=True)
    observaciones = models.TextField(null=True, blank=True)
    estado = models.CharField(max_length=100, null=True, blank=True)
    usuario = models.CharField(max_length=150, null=True, blank=True)

    valor = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    costo_mano_obra = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    fecha_pago = models.DateTimeField(null=True, blank=True)

    @property
    def clasificacion(self):
        from .models import CuentaB2B
        if self.cuenta_contable and CuentaB2B.objects.filter(cuenta=self.cuenta_contable).exists():
            return "B2B"
        return "B2C"

    # MONTO ORIGINAL DEL EXCEL (NO SE MODIFICA)
    valor_pago_original = models.IntegerField(null=True, blank=True)

    # MONTO QUE SE PUEDE EDITAR PARA PAGAR
    valor_pago_tecnico = models.IntegerField(null=True, blank=True)

    tiempo_trabajo_total = models.CharField(max_length=100, null=True, blank=True)
    cuenta_contable = models.CharField(max_length=20, null=True, blank=True)
    numero_incidencias_dia = models.IntegerField(default=0)

    ESTADO_PAGO_CHOICES = [
        ("aprobado", "Aprobado"),
        ("revision", "En Revisión"),
        ("rechazado", "Rechazado"),
    ]

    estado_pago = models.CharField(
        max_length=20,
        choices=ESTADO_PAGO_CHOICES,
        default="aprobado"
    )

    def __str__(self):
        cuenta = self.cuenta_contable if self.cuenta_contable else "Sin cuenta"
        return f"{self.numero} - {cuenta}"



class CuentaB2B(models.Model):
    cuenta = models.CharField(max_length=50)
    nombre = models.CharField(max_length=255, null=True, blank=True)
    kam = models.CharField(max_length=150, null=True, blank=True)

    

    def __str__(self):
        return f"{self.cuenta} - {self.nombre}"
    


class CECO(models.Model):
    cuenta = models.CharField(max_length=50)
    ceco = models.CharField(max_length=50)
    nombre = models.CharField(max_length=255)
    estado = models.CharField(max_length=20, null=True, blank=True)

    def __str__(self):
        return f"{self.cuenta} - {self.ceco} - {self.nombre}"

    class Meta:
        unique_together = ("cuenta", "ceco")  # 🔥 CLAVE REAL





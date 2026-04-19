from django.db import models
from django.utils import timezone
import hashlib
import re
import unicodedata


# =========================
# HELPERS DE NORMALIZACIÓN
# =========================

def quitar_tildes(texto: str) -> str:
    if not texto:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def normalizar_texto_base(valor: str) -> str:
    if not valor:
        return ""

    valor = quitar_tildes(valor)
    valor = valor.upper().strip()
    valor = valor.replace(".", "")
    valor = re.sub(r"\s+", " ", valor)
    return valor.strip()


def normalizar_nombre_persona(valor: str) -> str:
    valor = normalizar_texto_base(valor)

    if not valor:
        return ""

    # quitar prefijos típicos del Excel
    valor = re.sub(r"^T\s*INTERNO\s*-?\s*", "", valor)
    valor = re.sub(r"^T\s*EXTERNO\s*-?\s*", "", valor)

    # quitar razones sociales frecuentes si se usa para nombre
    valor = re.sub(r"\bEIRL\b", "", valor)
    valor = re.sub(r"\bSPA\b", "", valor)
    valor = re.sub(r"\bLTDA\b", "", valor)
    valor = re.sub(r"\bLIMITADA\b", "", valor)

    valor = re.sub(r"[^A-Z0-9 ]", "", valor)
    valor = re.sub(r"\s+", " ", valor)
    return valor.strip()


def normalizar_empresa(valor: str) -> str:
    valor = normalizar_texto_base(valor)

    if not valor:
        return ""

    valor = re.sub(r"[^A-Z0-9 ]", "", valor)
    valor = re.sub(r"\s+", " ", valor)
    return valor.strip()


def normalizar_rut(valor: str) -> str:
    if not valor:
        return ""

    valor = str(valor).strip().upper()
    valor = valor.replace(".", "").replace(" ", "")
    valor = re.sub(r"[^0-9K\-]", "", valor)

    # normalizar formato XXXXXXXX-X
    if "-" not in valor and len(valor) >= 2:
        valor = f"{valor[:-1]}-{valor[-1]}"

    return valor.strip()


def normalizar_cuenta(valor: str) -> str:
    if not valor:
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


# =========================
# ABSTRACT BASE
# =========================

class TimeStampedModel(models.Model):
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    fecha_actualizacion = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


# =========================
# CATÁLOGOS MAESTROS
# =========================

class CargaMensual(TimeStampedModel):
    """
    Catálogo lógico de periodo.
    Lo mantengo porque ya existe en tu sistema actual.
    """
    nombre = models.CharField(max_length=150)
    mes = models.IntegerField(null=True, blank=True)
    anio = models.IntegerField(null=True, blank=True)
    activa = models.BooleanField(default=True)

    class Meta:
        ordering = ["-anio", "-mes", "-id"]

    def __str__(self):
        if self.mes and self.anio:
            return f"{self.nombre} ({self.mes}/{self.anio})"
        return self.nombre


class Contratista(TimeStampedModel):
    ORIGEN_CHOICES = [
        ("json", "JSON"),
        ("excel", "Excel"),
        ("manual", "Manual"),
        ("revision", "Creado en revisión"),
    ]

    nombre = models.CharField(max_length=150)
    nombre_empresa = models.CharField(max_length=150, blank=True, null=True)

    nombre_normalizado = models.CharField(max_length=150, blank=True, default="", db_index=True)
    empresa_normalizada = models.CharField(max_length=150, blank=True, default="", db_index=True)

    rut = models.CharField(max_length=20, blank=True, null=True)
    rut_normalizado = models.CharField(max_length=20, blank=True, default="", db_index=True)

    fono = models.CharField(max_length=20, blank=True, null=True)
    correo = models.EmailField(blank=True, null=True)
    ciudad = models.CharField(max_length=100, blank=True, null=True)

    banco = models.CharField(max_length=50, blank=True, null=True)
    tipo_cuenta = models.CharField(max_length=50, blank=True, null=True)
    numero_cuenta = models.CharField(max_length=30, blank=True, null=True)

    categoria = models.CharField(max_length=50, blank=True, null=True)

    origen = models.CharField(max_length=20, choices=ORIGEN_CHOICES, default="manual")
    activo = models.BooleanField(default=True)
    requiere_revision = models.BooleanField(default=False)
    observacion_revision = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["nombre"]
        indexes = [
            models.Index(fields=["nombre_normalizado"]),
            models.Index(fields=["empresa_normalizada"]),
            models.Index(fields=["rut_normalizado"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["rut_normalizado"],
                condition=~models.Q(rut_normalizado=""),
                name="uq_contratista_rut_normalizado_no_vacio"
            )
        ]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = normalizar_nombre_persona(self.nombre)
        self.empresa_normalizada = normalizar_empresa(self.nombre_empresa or self.nombre)
        self.rut_normalizado = normalizar_rut(self.rut)
        super().save(*args, **kwargs)

    def __str__(self):
        empresa = self.nombre_empresa or self.nombre
        return f"{self.nombre} - {empresa}"


class AliasContratista(TimeStampedModel):
    alias = models.CharField(max_length=200, unique=True)
    alias_normalizado = models.CharField(max_length=200, unique=True, db_index=True)
    contratista = models.ForeignKey(
        Contratista,
        on_delete=models.CASCADE,
        related_name="aliases"
    )
    creado_en_revision = models.BooleanField(default=False)
    activo = models.BooleanField(default=True)

    class Meta:
        ordering = ["alias"]

    def save(self, *args, **kwargs):
        self.alias_normalizado = normalizar_nombre_persona(self.alias)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.alias} -> {self.contratista.nombre}"


class Tecnico(TimeStampedModel):
    TIPO_CHOICES = [
        ("interno", "Técnico Interno"),
        ("contratista", "Técnico Contratista"),
    ]

    CATEGORIA_CHOICES = [
        ("principal", "Principal"),
        ("remoto", "Remoto"),
    ]

    nombre = models.CharField(max_length=150, unique=True)
    nombre_normalizado = models.CharField(max_length=150, blank=True, default="", db_index=True)

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default="interno")
    categoria = models.CharField(max_length=20, choices=CATEGORIA_CHOICES, default="remoto")

    contratista = models.ForeignKey(
        Contratista,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="tecnicos"
    )

    activo = models.BooleanField(default=True)
    requiere_revision = models.BooleanField(default=False)

    class Meta:
        ordering = ["nombre"]

    def save(self, *args, **kwargs):
        self.nombre_normalizado = normalizar_nombre_persona(self.nombre)
        super().save(*args, **kwargs)

    @property
    def nombre_mostrar(self):
        return self.nombre or "-"

    def __str__(self):
        return f"{self.nombre} ({self.tipo}/{self.categoria})"


class AliasTecnico(TimeStampedModel):
    alias = models.CharField(max_length=200, unique=True)
    alias_normalizado = models.CharField(max_length=200, unique=True, db_index=True)
    tecnico = models.ForeignKey(
        Tecnico,
        on_delete=models.CASCADE,
        related_name="aliases"
    )
    activo = models.BooleanField(default=True)

    class Meta:
        ordering = ["alias"]

    def save(self, *args, **kwargs):
        self.alias_normalizado = normalizar_nombre_persona(self.alias)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.alias} -> {self.tecnico.nombre}"


class CuentaB2B(TimeStampedModel):
    cuenta = models.CharField(max_length=50, unique=True)
    cuenta_normalizada = models.CharField(max_length=50, unique=True, db_index=True)
    nombre = models.CharField(max_length=255, null=True, blank=True)
    kam = models.CharField(max_length=150, null=True, blank=True)
    activo = models.BooleanField(default=True)

    class Meta:
        ordering = ["cuenta"]

    def save(self, *args, **kwargs):
        self.cuenta_normalizada = normalizar_cuenta(self.cuenta)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.cuenta} - {self.nombre or ''}"


class CECO(TimeStampedModel):
    cuenta = models.CharField(max_length=50)
    cuenta_normalizada = models.CharField(max_length=50, blank=True, default="", db_index=True)

    ceco = models.CharField(max_length=50)
    nombre = models.CharField(max_length=255)
    estado = models.CharField(max_length=20, null=True, blank=True)

    activo = models.BooleanField(default=True)

    class Meta:
        ordering = ["cuenta", "ceco"]
        unique_together = ("cuenta_normalizada", "ceco")

    def save(self, *args, **kwargs):
        self.cuenta_normalizada = normalizar_cuenta(self.cuenta)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.cuenta_normalizada} - {self.ceco} - {self.nombre}"


# =========================
# CARGA DE ARCHIVOS / STAGING
# =========================

class ArchivoCarga(TimeStampedModel):
    ESTADO_CHOICES = [
        ("cargado", "Cargado"),
        ("procesando", "Procesando"),
        ("procesado", "Procesado"),
        ("procesado_con_observaciones", "Procesado con observaciones"),
        ("error", "Error"),
    ]

    carga_mensual = models.ForeignKey(
        CargaMensual,
        on_delete=models.CASCADE,
        related_name="archivos"
    )

    nombre_original = models.CharField(max_length=255)
    hash_archivo = models.CharField(max_length=64, unique=True, db_index=True)

    mes = models.IntegerField(null=True, blank=True)
    anio = models.IntegerField(null=True, blank=True)

    estado = models.CharField(max_length=40, choices=ESTADO_CHOICES, default="cargado")
    total_filas = models.IntegerField(default=0)
    filas_procesadas = models.IntegerField(default=0)
    filas_publicadas = models.IntegerField(default=0)
    filas_con_observacion = models.IntegerField(default=0)

    mensaje = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["-fecha_creacion"]

    def __str__(self):
        return f"{self.nombre_original} [{self.estado}]"

    @staticmethod
    def calcular_hash_desde_bytes(contenido: bytes) -> str:
        return hashlib.sha256(contenido).hexdigest()


class ServicioTecnicoRaw(TimeStampedModel):
    ESTADO_CHOICES = [
        ("pendiente", "Pendiente"),
        ("procesado", "Procesado"),
        ("publicado", "Publicado"),
        ("error", "Error"),
        ("revision", "Revisión"),
    ]

    archivo_carga = models.ForeignKey(
        ArchivoCarga,
        on_delete=models.CASCADE,
        related_name="filas_raw"
    )

    fila_numero = models.IntegerField()
    data = models.JSONField()

    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default="pendiente")
    procesado = models.BooleanField(default=False)
    publicado = models.BooleanField(default=False)
    requiere_revision = models.BooleanField(default=False)

    # Huellas mínimas para búsquedas rápidas y trazabilidad
    numero_ot = models.IntegerField(null=True, blank=True, db_index=True)
    tecnico_texto = models.CharField(max_length=200, blank=True, null=True)
    tecnico_normalizado = models.CharField(max_length=200, blank=True, default="", db_index=True)

    cuenta_texto = models.CharField(max_length=400, blank=True, null=True)
    cuenta_contable = models.CharField(max_length=50, blank=True, null=True, db_index=True)

    tipo_servicio = models.CharField(max_length=150, blank=True, null=True)
    fecha_visita = models.DateTimeField(null=True, blank=True)
    fecha_finalizacion = models.DateTimeField(null=True, blank=True)

    valor_pago_original_texto = models.CharField(max_length=100, blank=True, null=True)
    valor_pago_tecnico = models.IntegerField(null=True, blank=True)

    error = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["archivo_carga", "fila_numero"]
        unique_together = ("archivo_carga", "fila_numero")

    def __str__(self):
        return f"Raw #{self.fila_numero} - {self.archivo_carga.nombre_original}"


# =========================
# OBSERVACIONES / EXCEPCIONES
# =========================

class ObservacionImportacion(TimeStampedModel):
    ESTADO_CHOICES = [
        ("pendiente", "Pendiente"),
        ("resuelta", "Resuelta"),
        ("ignorada", "Ignorada"),
    ]

    TIPO_CHOICES = [
        ("contratista_ambiguo", "Contratista ambiguo"),
        ("contratista_no_encontrado", "Contratista no encontrado"),
        ("tecnico_sin_match", "Técnico sin match"),
        ("tecnico_ambiguo", "Técnico ambiguo"),
        ("cuenta_invalida", "Cuenta inválida"),
        ("ceco_no_encontrado", "CECO no encontrado"),
        ("fecha_invalida", "Fecha inválida"),
        ("monto_invalido", "Monto inválido"),
        ("dato_incompleto", "Dato incompleto"),
        ("duplicado", "Duplicado"),
        ("otro", "Otro"),
    ]

    raw = models.ForeignKey(
        ServicioTecnicoRaw,
        on_delete=models.CASCADE,
        related_name="observaciones"
    )

    tipo = models.CharField(max_length=50, choices=TIPO_CHOICES)
    detalle = models.TextField()
    valor_detectado = models.CharField(max_length=255, blank=True, null=True)
    sugerencia = models.CharField(max_length=255, blank=True, null=True)

    estado = models.CharField(max_length=20, choices=ESTADO_CHOICES, default="pendiente")
    resuelto_por = models.CharField(max_length=150, blank=True, null=True)
    fecha_resolucion = models.DateTimeField(blank=True, null=True)
    comentario_resolucion = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["estado", "-fecha_creacion"]

    def marcar_resuelta(self, usuario: str = "", comentario: str = ""):
        self.estado = "resuelta"
        self.resuelto_por = usuario or None
        self.comentario_resolucion = comentario or None
        self.fecha_resolucion = timezone.now()
        self.save(update_fields=[
            "estado",
            "resuelto_por",
            "comentario_resolucion",
            "fecha_resolucion",
            "fecha_actualizacion",
        ])

    def __str__(self):
        return f"{self.tipo} - {self.estado}"


# =========================
# DATOS OPERACIONALES FINALES
# =========================

class ServicioTecnico(TimeStampedModel):
    ESTADO_PAGO_CHOICES = [
        ("aprobado", "Aprobado"),
        ("revision", "En Revisión"),
        ("rechazado", "Rechazado"),
        ("no_cobrado", "No Cobrado"),
    ]

    carga = models.ForeignKey(
        CargaMensual,
        on_delete=models.CASCADE,
        related_name="servicios"
    )

    archivo_carga = models.ForeignKey(
        ArchivoCarga,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios_publicados"
    )

    raw = models.OneToOneField(
        ServicioTecnicoRaw,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicio_publicado"
    )

    contratista = models.ForeignKey(
        Contratista,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios"
    )

    numero = models.IntegerField(null=True, blank=True, db_index=True)

    fecha_creacion_origen = models.DateTimeField(null=True, blank=True)
    fecha_modificacion = models.DateTimeField(null=True, blank=True)
    fecha_visita = models.DateTimeField(null=True, blank=True)
    fecha_finalizacion = models.DateTimeField(null=True, blank=True)

    cuenta = models.CharField(max_length=400, null=True, blank=True)
    telefono = models.CharField(max_length=100, null=True, blank=True)

    tecnico = models.CharField(max_length=150, null=True, blank=True)  # respaldo texto
    tecnico_obj = models.ForeignKey(
        Tecnico,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios"
    )

    direccion = models.CharField(max_length=300, null=True, blank=True)
    provincia_estado = models.CharField(max_length=150, null=True, blank=True)
    localidad = models.CharField(max_length=150, null=True, blank=True)

    tipo_servicio = models.CharField(max_length=150, null=True, blank=True)
    servicio = models.CharField(max_length=300, null=True, blank=True)
    observaciones = models.TextField(null=True, blank=True)
    estado = models.CharField(max_length=100, null=True, blank=True)
    usuario = models.CharField(max_length=150, null=True, blank=True)

    valor = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    costo_mano_obra = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    fecha_pago = models.DateTimeField(null=True, blank=True)

    valor_pago_original = models.IntegerField(null=True, blank=True)
    valor_pago_tecnico = models.IntegerField(null=True, blank=True)

    tiempo_trabajo_total = models.CharField(max_length=100, null=True, blank=True)

    cuenta_contable = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    ceco = models.ForeignKey(
        CECO,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="servicios"
    )

    numero_incidencias_dia = models.IntegerField(default=0)
    numero_incidencias_60_dias = models.IntegerField(default=0)

    estado_pago = models.CharField(
        max_length=20,
        choices=ESTADO_PAGO_CHOICES,
        default="aprobado"
    )

    es_b2b = models.BooleanField(default=False)
    requiere_revision = models.BooleanField(default=False)

    class Meta:
        ordering = ["-fecha_finalizacion", "-id"]
        indexes = [
            models.Index(fields=["numero"]),
            models.Index(fields=["cuenta_contable"]),
            models.Index(fields=["estado_pago"]),
            models.Index(fields=["tipo_servicio"]),
            models.Index(fields=["fecha_finalizacion"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["raw"],
                condition=~models.Q(raw=None),
                name="uq_servicio_raw_unico"
            )
        ]

    @property
    def clasificacion(self):
        return "B2B" if self.es_b2b else "B2C"

    @property
    def tipo_tecnico(self):
        if self.tecnico_obj:
            return self.tecnico_obj.tipo
        return "contratista" if self.contratista_id else "interno"

    def __str__(self):
        cuenta = self.cuenta_contable if self.cuenta_contable else "Sin cuenta"
        return f"{self.numero} - {cuenta}"
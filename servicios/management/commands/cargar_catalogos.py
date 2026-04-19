import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from servicios.models import Contratista, CuentaB2B, CECO


def normalizar_cuenta(valor: str) -> str:
    if not valor:
        return ""

    valor = str(valor).strip().upper()

    if valor in ["", "NAN", "NONE", "NULL"]:
        return ""

    import re
    valor = re.sub(r"\s+", "", valor)

    match_fir = re.match(r"^FIR[-_]?([A-Z0-9]+)$", valor)
    if match_fir:
        return f"FIR-{match_fir.group(1)}"

    match_new = re.match(r"^NEW[-_]?SF[-_]?([A-Z0-9]+)$", valor)
    if match_new:
        return f"NEW-SF_{match_new.group(1)}"

    return valor


class Command(BaseCommand):
    help = "Carga catálogos maestros desde archivos JSON sin usar loaddata."

    def add_arguments(self, parser):
        parser.add_argument(
            "--contratistas",
            type=str,
            default="servicios/fixtures/contratistas.json",
            help="Ruta del archivo JSON de contratistas",
        )
        parser.add_argument(
            "--cuentas_b2b",
            type=str,
            default="servicios/fixtures/cuentas_b2b.json",
            help="Ruta del archivo JSON de cuentas B2B",
        )
        parser.add_argument(
            "--cecos",
            type=str,
            default="servicios/fixtures/cecos.json",
            help="Ruta del archivo JSON de CECO",
        )
        parser.add_argument(
            "--solo",
            type=str,
            choices=["contratistas", "cuentas_b2b", "cecos"],
            help="Carga solo un catálogo específico",
        )
        parser.add_argument(
            "--limpiar",
            action="store_true",
            help="Elimina los registros existentes del catálogo antes de cargar",
        )

    def handle(self, *args, **options):
        solo = options.get("solo")
        limpiar = options.get("limpiar")

        try:
            with transaction.atomic():
                if solo in [None, "contratistas"]:
                    self.cargar_contratistas(options["contratistas"], limpiar)

                if solo in [None, "cuentas_b2b"]:
                    self.cargar_cuentas_b2b(options["cuentas_b2b"], limpiar)

                if solo in [None, "cecos"]:
                    self.cargar_cecos(options["cecos"], limpiar)

        except Exception as e:
            raise CommandError(f"Error al cargar catálogos: {e}")

        self.stdout.write(self.style.SUCCESS("Carga de catálogos finalizada correctamente."))

    def leer_json(self, ruta):
        path = Path(ruta)

        if not path.exists():
            raise CommandError(f"No existe el archivo: {ruta}")

        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def cargar_contratistas(self, ruta, limpiar):
        data = self.leer_json(ruta)

        if limpiar:
            self.stdout.write("Limpiando contratistas...")
            Contratista.objects.all().delete()

        creados = 0
        actualizados = 0

        for item in data:
            fields = item.get("fields", {})

            nombre = fields.get("nombre")
            nombre_empresa = fields.get("nombre_empresa")

            if not nombre:
                self.stdout.write(self.style.WARNING("Contratista omitido: falta nombre"))
                continue

            defaults = {
                "nombre_empresa": nombre_empresa,
                "rut": fields.get("rut"),
                "fono": fields.get("fono"),
                "correo": fields.get("correo"),
                "ciudad": fields.get("ciudad"),
                "banco": fields.get("banco"),
                "tipo_cuenta": fields.get("tipo_cuenta"),
                "numero_cuenta": fields.get("numero_cuenta"),
                "categoria": fields.get("categoria"),
                "origen": "json",
                "activo": True,
                "requiere_revision": False,
                "observacion_revision": fields.get("observacion_revision"),
            }

            obj, creado = Contratista.objects.update_or_create(
                nombre=nombre,
                defaults=defaults,
            )

            if creado:
                creados += 1
            else:
                actualizados += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Contratistas -> creados: {creados}, actualizados: {actualizados}"
            )
        )

    def cargar_cuentas_b2b(self, ruta, limpiar):
        data = self.leer_json(ruta)

        if limpiar:
            self.stdout.write("Limpiando cuentas B2B...")
            CuentaB2B.objects.all().delete()

        creados = 0
        actualizados = 0
        omitidos = 0
        duplicados_en_json = 0

        vistos = set()

        for item in data:
            fields = item.get("fields", {})

            cuenta = fields.get("cuenta")
            if not cuenta:
                omitidos += 1
                continue

            cuenta_normalizada = normalizar_cuenta(cuenta)

            if not cuenta_normalizada:
                omitidos += 1
                continue

            if cuenta_normalizada in vistos:
                duplicados_en_json += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"Cuenta B2B duplicada en JSON omitida: {cuenta} -> {cuenta_normalizada}"
                    )
                )
                continue

            vistos.add(cuenta_normalizada)

            defaults = {
                "cuenta": cuenta_normalizada,
                "nombre": fields.get("nombre"),
                "kam": fields.get("kam"),
                "activo": True,
            }

            obj, creado = CuentaB2B.objects.update_or_create(
                cuenta_normalizada=cuenta_normalizada,
                defaults=defaults,
            )

            if creado:
                creados += 1
            else:
                actualizados += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Cuentas B2B -> creadas: {creados}, actualizadas: {actualizados}, omitidas: {omitidos}, duplicadas_json: {duplicados_en_json}"
            )
        )

    def cargar_cecos(self, ruta, limpiar):
        data = self.leer_json(ruta)

        if limpiar:
            self.stdout.write("Limpiando CECO...")
            CECO.objects.all().delete()

        creados = 0
        actualizados = 0
        omitidos = 0
        duplicados_en_json = 0

        vistos = set()

        for item in data:
            fields = item.get("fields", {})

            cuenta = fields.get("cuenta")
            ceco = fields.get("ceco")

            if not cuenta or not ceco:
                omitidos += 1
                continue

            cuenta_normalizada = normalizar_cuenta(cuenta)
            clave = (cuenta_normalizada, str(ceco).strip())

            if not cuenta_normalizada:
                omitidos += 1
                continue

            if clave in vistos:
                duplicados_en_json += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"CECO duplicado en JSON omitido: cuenta={cuenta} -> {cuenta_normalizada}, ceco={ceco}"
                    )
                )
                continue

            vistos.add(clave)

            defaults = {
                "cuenta": cuenta_normalizada,
                "nombre": fields.get("nombre"),
                "estado": fields.get("estado"),
                "activo": True,
            }

            obj, creado = CECO.objects.update_or_create(
                cuenta_normalizada=cuenta_normalizada,
                ceco=str(ceco).strip(),
                defaults=defaults,
            )

            if creado:
                creados += 1
            else:
                actualizados += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"CECO -> creados: {creados}, actualizados: {actualizados}, omitidos: {omitidos}, duplicados_json: {duplicados_en_json}"
            )
        )
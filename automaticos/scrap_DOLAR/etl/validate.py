"""
VALIDATE - Módulo de validación de datos DOLAR
"""
import logging

logger = logging.getLogger(__name__)


class ValidateDOLAR:
    def validate(self, datos: dict):
        if not datos:
            raise ValueError("[VALIDATE] No se extrajeron cotizaciones de dólar.")
        for tipo, df in datos.items():
            if df is None or df.empty:
                raise ValueError(f"[VALIDATE] DataFrame vacío para dólar '{tipo}'.")
            faltantes = [c for c in ['fecha', 'compra', 'venta'] if c not in df.columns]
            if faltantes:
                raise ValueError(f"[VALIDATE] '{tipo}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] OK — %d tipos de dólar validados.", len(datos))

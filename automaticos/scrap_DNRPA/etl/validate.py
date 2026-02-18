"""
VALIDATE - Módulo de validación de datos DNRPA
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateDNRPA:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame DNRPA vacío.")
        for col in ['fecha', 'id_provincia_indec', 'id_vehiculo', 'cantidad']:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna '{col}' faltante.")
        logger.info("[VALIDATE] OK — DNRPA: %d filas.", len(df))

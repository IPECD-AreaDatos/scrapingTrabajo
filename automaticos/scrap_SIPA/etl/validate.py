"""
VALIDATE - Módulo de validación de datos SIPA
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateSIPA:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame SIPA vacío.")
        for col in ['fecha', 'id_provincia', 'id_tipo_registro']:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna '{col}' faltante.")
        logger.info("[VALIDATE] OK — SIPA: %d filas.", len(df))

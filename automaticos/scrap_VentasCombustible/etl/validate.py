"""
VALIDATE - Módulo de validación de datos VentasCombustible
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateVentasCombustible:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame VentasCombustible vacío.")
        for col in ['fecha', 'cantidad']:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna '{col}' faltante.")
        logger.info("[VALIDATE] OK — VentasCombustible: %d filas.", len(df))

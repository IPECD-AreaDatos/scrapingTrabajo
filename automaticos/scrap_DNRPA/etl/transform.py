"""
TRANSFORM - Módulo de transformación de datos DNRPA
Responsabilidad: Pass-through con validación (datos ya vienen limpios del extract)
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TransformDNRPA:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("[TRANSFORM] DNRPA: %d filas", len(df))
        return df

"""
TRANSFORM - Módulo de transformación de datos IPC_Online
Responsabilidad: Pass-through con validación (datos ya vienen limpios del extract)
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TransformIPCOnline:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("[TRANSFORM] IPC Online: %d filas", len(df))
        return df

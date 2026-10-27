"""
VALIDATE - Módulo de validación de datos CanastaBasica
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateCanastaBasica:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame CanastaBasica vacío.")
        
        if 'precio_normal' not in df.columns:
            raise ValueError("[VALIDATE] Columna 'precio_normal' faltante.")
        
        df['precio_normal'] = pd.to_numeric(df['precio_normal'], errors='coerce').fillna(0)
        validos = len(df[df['precio_normal'] > 0])
        
        if validos == 0:
            raise ValueError("[VALIDATE] No hay ningún producto con precio > 0.")
            
        logger.info("[VALIDATE] OK — CanastaBasica: %d filas válidas.", validos)
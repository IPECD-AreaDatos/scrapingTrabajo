import logging
import pandas as pd

logger = logging.getLogger(__name__)

class Validate:
    def validate(self, df: pd.DataFrame):
        """Valida que los datos de embarazo tengan la estructura y calidad mínima."""
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío o nulo.")

        # Columnas críticas para el tablero de salud
        # (Usamos los nombres ya normalizados por el Transform)
        columnas_criticas = ['dni', 'fecha']
        
        for col in columnas_criticas:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna crítica faltante: '{col}'")
            
            # Verificar que no vengan todos los datos nulos en esas columnas
            if df[col].isnull().all():
                raise ValueError(f"[VALIDATE] La columna '{col}' está totalmente vacía.")

        logger.info("[VALIDATE] OK — %d filas validadas correctamente.", len(df))
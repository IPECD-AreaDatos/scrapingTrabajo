import logging
import pandas as pd

logger = logging.getLogger(__name__)
class Validate:
    def validate(self, df: pd.DataFrame):
        """Valida que los datos tengan la estructura y calidad mínima."""
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío o nulo.")

        # --- Lógica inteligente para encontrar columnas críticas ---
        # 1. Buscamos el DNI (puede ser 'dni' o 'afidni')
        col_dni = next((c for c in ['dni', 'afidni'] if c in df.columns), None)
        
        # 2. Buscamos la Fecha (puede ser 'fecha', 'afifechanac' o 'fecha_de_notif')
        col_fecha = next((c for c in ['fecha', 'afifechanac', 'fecha_de_notif'] if c in df.columns), None)

        # Validamos DNI (Obligatorio para todas las fuentes)
        if not col_dni:
            raise ValueError(f"[VALIDATE] No se encontró la columna de DNI. Columnas actuales: {df.columns.tolist()}")
        
        if df[col_dni].isnull().all():
            raise ValueError(f"[VALIDATE] La columna '{col_dni}' está totalmente vacía.")

        # Validamos Fecha (Si existe, que no esté vacía)
        if col_fecha:
            if df[col_fecha].isnull().all():
                logger.warning(f"[VALIDATE] Alerta: La columna de fecha '{col_fecha}' viene vacía.")
        else:
            logger.warning("[VALIDATE] No se detectó columna de fecha estándar para validar.")

        logger.info("[VALIDATE] OK — %d filas validadas correctamente.", len(df))
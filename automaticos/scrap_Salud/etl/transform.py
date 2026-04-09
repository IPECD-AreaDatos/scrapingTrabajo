import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Columnas que deberían ser fechas
COLUMNAS_FECHAS = [
    'Fecha Notificacion', 'Fecha Derivacion', 'Fecha Nac.', 
    'Fecha inicio embarazo', 'Fecha diagnostico emb.', 'FPP (SUMAR)', 'FUMLocalidad'
]

# Columnas que deberían ser numéricas
COLUMNAS_NUMERICAS = ['EG (semanas)', 'Edad paciente', 'Gestas previas', 'Cant. controles', 'IMC']

class Transform:
    """Transforma el DataFrame de Salud/Embarazo."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        logger.info("[TRANSFORM] Iniciando transformación de %d filas...", len(df))
        df = df.copy()

        # 1. Limpieza de nombres de columnas (evitar problemas en PostgreSQL)
        df.columns = [col.strip().replace(' ', '_').replace('.', '').replace('/', '_').lower() for col in df.columns]

        # 2. Formatear Fechas
        for col in df.columns:
            if any(key.lower() in col for key in ['fecha', 'fpp', 'fum']):
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 3. Formatear Numéricos
        for col in df.columns:
            if any(key.lower() in col for key in ['edad', 'eg_semanas', 'gestas', 'controles', 'imc']):
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        # 4. Limpieza de Textos (quitar espacios extra)
        text_cols = df.select_dtypes(include=['object']).columns
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', ''], np.nan)

        logger.info("[TRANSFORM] Transformación completada.")
        return df
"""
TRANSFORM - Módulo de transformación de datos Índice de Salarios
Responsabilidad: Leer el CSV y limpiar los datos
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'indice_salarios.csv'


class TransformIndiceSalarios:
    """Transforma el CSV del Índice de Salarios."""

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        """
        Lee el CSV, limpia caracteres especiales y convierte fechas.

        Returns:
            pd.DataFrame con columnas en minúsculas y fechas en datetime
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Leyendo CSV: %s", ruta_archivo)
        df = pd.read_csv(ruta_archivo, delimiter=';')

        # Reemplazar comas por puntos y convertir a float
        for col in df.columns:
            try:
                df[col] = df[col].str.replace(',', '.').astype(float)
            except Exception:
                pass

        # Convertir fechas y renombrar de 'periodo' a 'fecha'
        df['fecha'] = pd.to_datetime(df['periodo'], format='%d/%m/%Y')
        df = df.drop(columns=['periodo']) # Eliminamos el nombre viejo

        # Columnas a minúsculas
        df.columns = [c.lower() for c in df.columns]

        logger.info("[TRANSFORM] DataFrame generado: %d filas", len(df))
        return df

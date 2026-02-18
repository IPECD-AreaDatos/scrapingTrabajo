"""
TRANSFORM - Módulo de transformación de datos VentasCombustible
Responsabilidad: Leer el CSV, filtrar Corrientes, crear columna fecha
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')

PROVINCIAS_NO_DESEADAS = ['S/D', 'no aplica', 'Provincia']
DICT_PROVINCIAS = {
    'Estado Nacional': 1, 'Capital Federal': 2, 'Buenos Aires': 6,
    'Catamarca': 10, 'Chaco': 22, 'Chubut': 26, 'Córdoba': 14,
    'Corrientes': 18, 'Entre Rios': 30, 'Formosa': 34, 'Jujuy': 38,
    'La Pampa': 42, 'La Rioja': 46, 'Mendoza': 50, 'Misiones': 54,
    'Neuquén': 58, 'Rio Negro': 62, 'Salta': 66, 'San Juan': 70,
    'San Luis': 74, 'Santa Cruz': 78, 'Santa Fe': 82,
    'Santiago del Estero': 86, 'Tierra del Fuego': 94, 'Tucuman': 90,
}
COLS_ELIMINAR = ['empresa', 'tipodecomercializacion', 'subtipodecomercializacion', 'pais', 'indice_tiempo']


class TransformVentasCombustible:
    """Transforma el CSV de ventas de combustible."""

    def transform(self, ruta: str = None) -> pd.DataFrame:
        """
        Lee y transforma el CSV de combustible, filtrando por Corrientes.

        Returns:
            pd.DataFrame con columnas: fecha, provincia, producto, cantidad
        """
        if ruta is None:
            ruta = os.path.join(FILES_DIR, 'ventas_combustible.csv')

        logger.info("[TRANSFORM] Leyendo CSV: %s", ruta)
        df = pd.read_csv(ruta)

        # Eliminar columnas innecesarias
        df = df.drop(columns=[c for c in COLS_ELIMINAR if c in df.columns])

        # Crear columna fecha
        df['fecha'] = pd.to_datetime(df['anio'].astype(str) + '-' + df['mes'].astype(str) + '-01')
        df = df.drop(columns=['anio', 'mes'])
        df.insert(0, 'fecha', df.pop('fecha'))

        # Filtrar y mapear provincias
        df = df[~df['provincia'].isin(PROVINCIAS_NO_DESEADAS)]
        df['provincia'] = df['provincia'].replace(DICT_PROVINCIAS).infer_objects(copy=False)
        df = df[df['provincia'] == 18]  # Solo Corrientes

        # Eliminar columna unidad si existe
        if 'unidad' in df.columns:
            df = df.drop(columns=['unidad'])

        logger.info("[TRANSFORM] VentasCombustible: %d filas (Corrientes)", len(df))
        return df

    def suma_por_fecha(self, df: pd.DataFrame) -> float:
        """Retorna la suma de cantidad para la última fecha disponible."""
        ultima_fecha = df['fecha'].max()
        return df[df['fecha'] == ultima_fecha]['cantidad'].sum()

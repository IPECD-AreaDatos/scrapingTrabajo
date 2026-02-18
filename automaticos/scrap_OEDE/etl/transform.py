"""
TRANSFORM - Módulo de transformación de datos OEDE
Responsabilidad: Construir el DataFrame consolidado de todas las provincias
"""
import os
import logging
import pandas as pd
from unidecode import unidecode
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'ev_remun_trab_reg_por_sector.xlsx'

PROVINCIAS = {
    'GBA': 1, 'CABA': 2, 'Resto Pcia. Bs. As.': 6, 'Catamarca': 10,
    'Córdoba': 14, 'Corrientes': 18, 'Chaco': 22, 'Chubut': 26,
    'Entre Ríos': 30, 'Formosa': 34, 'Jujuy': 38, 'La Pampa': 42,
    'La Rioja': 46, 'Mendoza': 50, 'Misiones': 54, 'Neuquén': 58,
    'Río Negro': 62, 'Salta': 66, 'San Juan': 70, 'San Luis': 74,
    'Santa Cruz': 78, 'Santa Fe': 82, 'Santiago del Estero': 86,
    'Tierra del Fuego': 90, 'Tucumán': 94
}


class TransformOEDE:
    """Transforma el Excel de OEDE en un DataFrame consolidado por provincia."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self._engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"
        )
        self._diccionario = None

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        """
        Construye el DataFrame final con datos de todas las provincias.

        Returns:
            pd.DataFrame con columnas [fecha, id_provincia, id_categoria, id_subcategoria, valor]
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Construyendo diccionario desde BD...")
        self._construir_dic()

        logger.info("[TRANSFORM] Procesando %d provincias...", len(PROVINCIAS))
        dfs = []
        for provincia, id_prov in PROVINCIAS.items():
            logger.info("[TRANSFORM] Procesando: %s (id=%d)", provincia, id_prov)
            df = self._construir_df(ruta_archivo, provincia, id_prov)
            dfs.append(df)

        df_final = pd.concat(dfs, ignore_index=True)
        logger.info("[TRANSFORM] DataFrame final: %d filas", len(df_final))
        return df_final

    def _construir_dic(self):
        tabla = pd.read_sql_query("SELECT * FROM OEDE_diccionario", con=self._engine)
        diccionario = {}
        for _, row in tabla.iterrows():
            clave = self._formatear_key(row['nombre'])
            valor = [row['id_categoria'], int(row['id_subcategoria'])]
            diccionario.setdefault(clave, []).append(valor)
        self._diccionario = diccionario

    def _construir_df(self, ruta_archivo: str, provincia: str, id_prov: int) -> pd.DataFrame:
        df = pd.read_excel(ruta_archivo, sheet_name=provincia, skiprows=3)
        df = df.iloc[:70]
        df = df.drop(columns=['Rama de Actividad'])
        df.rename(columns={'Unnamed: 1': 'claves_listas'}, inplace=True)
        df['claves_listas'] = df['claves_listas'].apply(self._formatear_key)

        df[['id_categoria', 'id_subcategoria']] = df.apply(
            lambda row: pd.Series(self._mapear_clave(row['claves_listas'], row.name)),
            axis=1
        )
        df = df.drop('claves_listas', axis=1)

        df_t = pd.melt(
            df,
            id_vars=['id_categoria', 'id_subcategoria'],
            var_name='fecha',
            value_name='valor'
        )
        df_t['fecha']  = pd.to_datetime(df_t['fecha'], errors='coerce')
        df_t['valor']  = pd.to_numeric(df_t['valor'], errors='coerce')
        df_t['id_provincia'] = id_prov
        return df_t[['fecha', 'id_provincia', 'id_categoria', 'id_subcategoria', 'valor']]

    def _mapear_clave(self, clave: str, index: int) -> list:
        if 'calzado' in clave or 'cuero' in clave:
            return ['D', 19]
        if 'edicion' in clave:
            return ['D', 22]
        valores = self._diccionario.get(clave, [[None, None]])
        if len(valores) > 1:
            return valores[1] if index >= 40 else valores[0]
        return valores[0]

    @staticmethod
    def _formatear_key(key: str) -> str:
        return unidecode(key.lower()).replace(',', '').replace('.', '').replace(' ', '')

    def close(self):
        if self._engine:
            self._engine.dispose()

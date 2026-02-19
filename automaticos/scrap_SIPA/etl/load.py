"""
LOAD - Módulo de carga de datos SIPA
Responsabilidad: Cargar a MySQL con TRUNCATE + INSERT si hay datos nuevos, y calcular analytics
"""
import logging
import pymysql
import pandas as pd
from numpy import trunc
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = 'sipa_valores'


class LoadSIPA:
    """Carga los datos de SIPA a MySQL (truncate + replace si hay diferencias)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._conn    = None
        self._cursor  = None
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga el DataFrame si la fecha máxima del Excel supera la de la BD.

        Returns:
            bool: True si se cargaron datos nuevos
        """
        self._connect()

        # Comparar por fecha máxima (más robusto que por conteo de filas)
        self._cursor.execute(f"SELECT MAX(fecha) FROM {TABLA}")
        fecha_bdd = self._cursor.fetchone()[0]
        fecha_df  = pd.to_datetime(df['fecha']).max()

        logger.info("[LOAD] Última fecha en BD: %s | Última fecha en Excel: %s", fecha_bdd, fecha_df.date())

        if fecha_bdd is not None and pd.to_datetime(fecha_bdd) >= fecha_df:
            logger.info("[LOAD] Sin datos nuevos en '%s'. La BD ya está actualizada.", TABLA)
            self._close()
            return False

        # TRUNCATE + INSERT
        self._cursor.execute(f"TRUNCATE {TABLA}")
        df.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
        self._conn.commit()
        logger.info("[LOAD] %d filas cargadas en '%s'.", count_df, TABLA)

        # Analytics
        self._table_analytics_sipa()
        self._table_analytics_sipa_nea()
        self._close()
        return True

    def _connect(self):
        self._conn   = pymysql.connect(host=self.host, user=self.user,
                                       password=self.password, database=self.database)
        self._cursor = self._conn.cursor()

    def _close(self):
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
        if self._conn:
            try:
                self._conn.commit()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
        self._cursor = None
        self._conn = None

    def _get_engine(self, db=None):
        db = db or self.database
        return create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{db}")

    def _table_analytics_sipa(self):
        df = pd.DataFrame()
        self._get_percentages(df)
        self._get_variances_nation(df)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        engine = self._get_engine('dwh_economico')
        df.to_sql(name="empleo_nacional_porcentajes_variaciones", con=engine, if_exists='replace', index=True)
        logger.info("[LOAD] Analytics nacionales actualizados.")

    def _get_percentages(self, df):
        df_bdd = pd.read_sql("SELECT * FROM sipa_valores WHERE id_provincia = 1", self._conn)
        df['fecha'] = list(df_bdd['fecha'][df_bdd['id_tipo_registro'] == 8])
        df['empleo_total'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 8])
        df['empleo_total'] = df['empleo_total'].apply(lambda x: trunc(x * 1000) / 1000)
        for col, tipo in zip([
            'empleo_privado', 'empleo_publico', 'empleo_casas_particulares',
            'empleo_independiente_autonomo', 'empleo_independiente_monotributo', 'empleo_monotributo_social'
        ], [2, 3, 4, 5, 6, 7]):
            df[col] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == tipo])
            df[f'p_{col}'] = (df[col] * 100) / df['empleo_total']

    def _get_variances_nation(self, df):
        df['fecha'] = pd.to_datetime(df['fecha'])
        for col in ['empleo_total', 'empleo_privado']:
            df[f'vmensual_{col}'] = ((df[col] / df[col].shift(1)) - 1) * 100
            df[f'vinter_{col}']   = ((df[col] / df[col].shift(12)) - 1) * 100
            df[f'vacum_{col}']    = float('nan')
        for anio in sorted(df['fecha'].dt.year.unique()):
            dic = df[(df['fecha'].dt.year == anio - 1) & (df['fecha'].dt.month == 12)]
            if not dic.empty:
                for col in ['empleo_total', 'empleo_privado']:
                    base = dic[col].values[0]
                    df.loc[df['fecha'].dt.year == anio, f'vacum_{col}'] = (
                        (df[col][df['fecha'].dt.year == anio] / base) - 1) * 100

    def _table_analytics_sipa_nea(self):
        df = pd.DataFrame()
        self._get_variances_nea(df)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        engine = self._get_engine('dwh_economico')
        df.to_sql(name="empleo_nea_variaciones", con=engine, if_exists='replace', index=True)
        logger.info("[LOAD] Analytics NEA actualizados.")

    def _get_variances_nea(self, df):
        provincias = {18: 'corrientes', 54: 'misiones', 22: 'chaco', 34: 'formosa'}
        query = "SELECT fecha, id_provincia, cantidad_con_estacionalidad FROM sipa_valores WHERE id_provincia IN (18, 22, 34, 54)"
        df_bdd = pd.read_sql(query, self._conn)
        df['fecha'] = sorted(set(pd.to_datetime(df_bdd['fecha'])))
        for idp, nombre in provincias.items():
            df[f'total_{nombre}'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == idp] * 1000)
        df['total_nea'] = sum(df[f'total_{p}'] for p in provincias.values())
        for prov in list(provincias.values()) + ['nea']:
            df[f'vmensual_{prov}'] = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(1) - 1) * 100
            df[f'vinter_{prov}']   = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(12) - 1) * 100
            df[f'vacum_{prov}']    = float('nan')
        for anio in sorted(df['fecha'].dt.year.unique()):
            dic = df[(df['fecha'].dt.year == anio - 1) & (df['fecha'].dt.month == 12)]
            if not dic.empty:
                for prov in list(provincias.values()) + ['nea']:
                    base = dic[f'total_{prov}'].values[0]
                    df.loc[df['fecha'].dt.year == anio, f'vacum_{prov}'] = (
                        (df[f'total_{prov}'][df['fecha'].dt.year == anio] / base) - 1) * 100

    def close(self):
        self._close()
        if self._engine:
            self._engine.dispose()

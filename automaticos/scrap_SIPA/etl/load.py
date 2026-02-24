"""
LOAD - Módulo de carga de datos SIPA
Responsabilidad: Cargar a PostgreSQL con TRUNCATE + INSERT si hay datos nuevos, y calcular analytics
"""
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Configuración de tablas para Postgres
TABLA = 'sipa'
DWH_DB = 'dwh_economico'

class LoadSIPA:
    """Carga los datos de SIPA a PostgreSQL (truncate + replace incremental)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        # URL para PostgreSQL
        self.url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{database}"
        self._engine = create_engine(self.url)

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga el DataFrame si la fecha máxima del Excel supera la de la BD.
        Returns:
            bool: True si se cargaron datos nuevos
        """
        # Comparar por fecha máxima
        with self._engine.connect() as conn:
            res = conn.execute(text(f"SELECT MAX(fecha) FROM {TABLA}"))
            fecha_bdd = res.scalar()
        
        fecha_df = pd.to_datetime(df['fecha']).max()

        logger.info("[LOAD] Última fecha en BD: %s | Última fecha en Excel: %s", fecha_bdd, fecha_df.date())

        if fecha_bdd is not None and pd.to_datetime(fecha_bdd).date() >= fecha_df.date():
            logger.info("[LOAD] Sin datos nuevos en '%s'. La BD ya está actualizada.", TABLA)
            return False

        # TRUNCATE + INSERT usando una transacción (begin)
        # Se usa CASCADE por si existen Foreign Keys que dependan de esta tabla
        with self._engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TABLA} CASCADE"))
            df.to_sql(name=TABLA, con=conn, if_exists='append', index=False, method='multi')
        
        logger.info("[LOAD] %d filas cargadas en '%s'.", len(df), TABLA)

        # Analytics
        self._table_analytics_sipa()
        self._table_analytics_sipa_nea()
        return True

    def _get_engine_dwh(self):
        # Motor para el Data Warehouse en Postgres
        url_dwh = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:5432/{DWH_DB}"
        return create_engine(url_dwh)

    def _table_analytics_sipa(self):
        df_ana = pd.DataFrame()
        self._get_percentages(df_ana)
        self._get_variances_nation(df_ana)
        
        df_ana['fecha'] = pd.to_datetime(df_ana['fecha']).dt.date
        engine_dwh = self._get_engine_dwh()
        
        # En Postgres usamos index=False para evitar columnas 'index' basura
        df_ana.to_sql(name="empleo_nacional_porcentajes_variaciones", con=engine_dwh, if_exists='replace', index=False)
        logger.info("[LOAD] Analytics nacionales actualizados en Postgres.")

    def _get_percentages(self, df):
        # id_provincia 1 suele ser Total Nación o GBA según tu script anterior
        query = f"SELECT * FROM {TABLA} WHERE id_provincia = 1"
        df_bdd = pd.read_sql(query, con=self._engine)
        
        # Filtramos por id_registro 8 (Total)
        df['fecha'] = list(df_bdd['fecha'][df_bdd['id_registro'] == 8])
        df['empleo_total'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_registro'] == 8])
        
        # Truncamiento a 3 decimales
        df['empleo_total'] = df['empleo_total'].apply(lambda x: np.trunc(x * 1000) / 1000)
        
        mapping = [
            ('empleo_privado', 2), ('empleo_publico', 3), ('empleo_casas_particulares', 4),
            ('empleo_independiente_autonomo', 5), ('empleo_independiente_monotributo', 6), 
            ('empleo_monotributo_social', 7)
        ]
        
        for col, tipo in mapping:
            df[col] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_registro'] == tipo])
            df[f'p_{col}'] = (df[col] * 100) / df['empleo_total']

    def _get_variances_nation(self, df):
        df['fecha'] = pd.to_datetime(df['fecha'])
        for col in ['empleo_total', 'empleo_privado']:
            df[f'vmensual_{col}'] = ((df[col] / df[col].shift(1)) - 1) * 100
            df[f'vinter_{col}']   = ((df[col] / df[col].shift(12)) - 1) * 100
            df[f'vacum_{col}']    = np.nan
            
        for anio in sorted(df['fecha'].dt.year.unique()):
            dic = df[(df['fecha'].dt.year == anio - 1) & (df['fecha'].dt.month == 12)]
            if not dic.empty:
                for col in ['empleo_total', 'empleo_privado']:
                    base = dic[col].values[0]
                    mask = df['fecha'].dt.year == anio
                    df.loc[mask, f'vacum_{col}'] = ((df[col][mask] / base) - 1) * 100

    def _table_analytics_sipa_nea(self):
        df_nea = pd.DataFrame()
        self._get_variances_nea(df_nea)
        df_nea['fecha'] = pd.to_datetime(df_nea['fecha']).dt.date
        
        engine_dwh = self._get_engine_dwh()
        df_nea.to_sql(name="empleo_nea_variaciones", con=engine_dwh, if_exists='replace', index=False)
        logger.info("[LOAD] Analytics NEA actualizados en Postgres.")

    def _get_variances_nea(self, df):
        # IDs de provincias para el NEA (Corrientes es 18)
        provincias = {18: 'corrientes', 54: 'misiones', 22: 'chaco', 34: 'formosa'}
        ids_nea = tuple(provincias.keys())
        
        query = f"SELECT fecha, id_provincia, cantidad_con_estacionalidad FROM {TABLA} WHERE id_provincia IN {ids_nea}"
        df_bdd = pd.read_sql(query, con=self._engine)
        
        df['fecha'] = sorted(set(pd.to_datetime(df_bdd['fecha'])))
        
        for idp, nombre in provincias.items():
            # El factor 1000 depende de si el Excel viene expresado en miles
            df[f'total_{nombre}'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == idp] * 1000)
            
        df['total_nea'] = df[[f'total_{p}' for p in provincias.values()]].sum(axis=1)
        
        for prov in list(provincias.values()) + ['nea']:
            df[f'vmensual_{prov}'] = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(1) - 1) * 100
            df[f'vinter_{prov}']   = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(12) - 1) * 100
            df[f'vacum_{prov}']    = np.nan
            
        for anio in sorted(df['fecha'].dt.year.unique()):
            dic = df[(df['fecha'].dt.year == anio - 1) & (df['fecha'].dt.month == 12)]
            if not dic.empty:
                for prov in list(provincias.values()) + ['nea']:
                    base = dic[f'total_{prov}'].values[0]
                    mask = df['fecha'].dt.year == anio
                    df.loc[mask, f'vacum_{prov}'] = ((df[f'total_{prov}'][mask] / base) - 1) * 100

    def close(self):
        if self._engine:
            self._engine.dispose()
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from numpy import trunc


class conexionBaseDatos:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cursor = None
        self.conn = None

    def set_database(self, new_name):
        self.database = new_name

    def connect_db(self):
        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()

    def close_connections(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def load_all(self, df):
        self.connect_db()

        if self.should_reload(df):
            self.replace_table("sipa_valores", df)
            self.table_analytics_sipa()
            self.table_analytics_sipa_nea()
            return True

        print("\n - No existen datos nuevos de SIPA para cargar \n")
        return False

    def should_reload(self, df):
        self.cursor.execute("SELECT COUNT(*) FROM sipa_valores")
        count_bdd = self.cursor.fetchone()[0]
        count_df = len(df)
        print(f"df: {count_df} base: {count_bdd}")
        return count_df > count_bdd

    def replace_table(self, table_name, df):
        self.cursor.execute(f"TRUNCATE {table_name}")
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        self.conn.commit()

    def table_analytics_sipa(self):
        df = pd.DataFrame()
        self.get_percentages(df)
        self.get_variances_nation(df)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/dwh_economico")
        df.to_sql(name="empleo_nacional_porcentajes_variaciones", con=engine, if_exists='replace', index=True)
        self.conn.commit()

    def get_percentages(self, df):
        df_bdd = pd.read_sql("SELECT * FROM sipa_valores WHERE id_provincia = 1", self.conn)
        df['fecha'] = list(df_bdd['fecha'][df_bdd['id_tipo_registro'] == 8])
        df['empleo_total'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 8])
        df['empleo_total'] = df['empleo_total'].apply(lambda x: trunc(x * 1000) / 1000)

        for col, tipo in zip([
            'empleo_privado', 'empleo_publico', 'empleo_casas_particulares',
            'empleo_independiente_autonomo', 'empleo_independiente_monotributo', 'empleo_monotributo_social'
        ], [2, 3, 4, 5, 6, 7]):
            df[col] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == tipo])
            df[f'p_{col}'] = (df[col] * 100) / df['empleo_total']

    def get_variances_nation(self, df):
        df['fecha'] = pd.to_datetime(df['fecha'])
        df['vmensual_empleo_total'] = ((df['empleo_total'] / df['empleo_total'].shift(1)) - 1) * 100
        df['vinter_empleo_total'] = ((df['empleo_total'] / df['empleo_total'].shift(12)) - 1) * 100
        df['vmensual_empleo_privado'] = ((df['empleo_privado'] / df['empleo_privado'].shift(1)) - 1) * 100
        df['vinter_empleo_privado'] = ((df['empleo_privado'] / df['empleo_privado'].shift(12)) - 1) * 100

        df['vacum_empleo_total'] = float('nan')
        df['vacum_empleo_privado'] = float('nan')
        for anio in sorted(df['fecha'].dt.year.unique()):
            diciembre = df[(df['fecha'].dt.year == anio - 1) & (df['fecha'].dt.month == 12)]
            if not diciembre.empty:
                total = diciembre['empleo_total'].values[0]
                privado = diciembre['empleo_privado'].values[0]
                df.loc[df['fecha'].dt.year == anio, 'vacum_empleo_total'] = ((df['empleo_total'][df['fecha'].dt.year == anio] / total) - 1) * 100
                df.loc[df['fecha'].dt.year == anio, 'vacum_empleo_privado'] = ((df['empleo_privado'][df['fecha'].dt.year == anio] / privado) - 1) * 100

    def table_analytics_sipa_nea(self):
        df = pd.DataFrame()
        self.get_variances_nea(df)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/dwh_economico")
        df.to_sql(name="empleo_nea_variaciones", con=engine, if_exists='replace', index=True)
        self.conn.commit()

    def get_variances_nea(self, df):
        provincias = {18: 'corrientes', 54: 'misiones', 22: 'chaco', 34: 'formosa'}
        query = "SELECT fecha, id_provincia, cantidad_con_estacionalidad FROM sipa_valores WHERE id_provincia IN (18, 22, 34, 54)"
        df_bdd = pd.read_sql(query, self.conn)

        df['fecha'] = sorted(set(pd.to_datetime(df_bdd['fecha'])))
        for idp, nombre in provincias.items():
            df[f'total_{nombre}'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == idp] * 1000)

        df['total_nea'] = sum(df[f'total_{prov}'] for prov in provincias.values())

        for prov in provincias.values():
            df[f'vmensual_{prov}'] = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(1) - 1) * 100
            df[f'vinter_{prov}'] = (df[f'total_{prov}'] / df[f'total_{prov}'].shift(12) - 1) * 100
            df[f'vacum_{prov}'] = float('nan')

        df['vmensual_nea'] = (df['total_nea'] / df['total_nea'].shift(1) - 1) * 100
        df['vinter_nea'] = (df['total_nea'] / df['total_nea'].shift(12) - 1) * 100
        df['vacum_nea'] = float('nan')

        for anio in sorted(df['fecha'].dt.year.unique()):
            diciembre = df[df['fecha'].dt.year == anio - 1]
            diciembre = diciembre[diciembre['fecha'].dt.month == 12]
            if not diciembre.empty:
                for prov in list(provincias.values()) + ['nea']:
                    base = diciembre[f'total_{prov}'].values[0]
                    df.loc[df['fecha'].dt.year == anio, f'vacum_{prov}'] = ((df[f'total_{prov}'][df['fecha'].dt.year == anio] / base) - 1) * 100

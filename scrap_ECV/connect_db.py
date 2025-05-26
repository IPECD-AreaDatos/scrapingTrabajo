from sqlalchemy import create_engine
import pandas as pd

class connect_db:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:3306/{database}")

    def _should_update(self, df, table_name):
        with self.engine.connect() as connection:
            result = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", connection)
            filas_bd = result.iloc[0, 0]
        return len(df) != filas_bd

    def _replace_table(self, df, table_name):
        df.to_sql(name=table_name, con=self.engine, if_exists='replace', index=False)
        print(f"✅ Tabla {table_name} reemplazada con {len(df)} registros.")

    def connect_db_tasas(self, df):
        table_name = 'ecv_tasas'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_tasas.")

    def connect_db_trabajo(self, df):
        table_name = 'ecv_trabajo'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_trabajo.")

    def connect_db_trabajo_quintiles(self, df):
        table_name = 'ecv_trabajo_quintiles'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_trabajo_quintiles.")

    def connect_db_salud_cobertura(self, df):
        table_name = 'ecv_salud_cobertura'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_salud_cobertura.")

    def connect_db_salud_consulta_establecimiento(self, df):
        table_name = 'ecv_salud_consulta_establecimiento'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_salud_consulta_establecimiento.")

    def connect_db_salud_quintil_consulta(self, df):
        table_name = 'ecv_salud_quintil_consulta'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_salud_quintil_consulta.")

    def connect_db_salud_quintil_cobertura_est(self, df):
        table_name = 'ecv_salud_quintil_cobertura_est'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_salud_quintil_cobertura_est.")

    def connect_db_educacion(self, df):
        table_name = 'ecv_educacion'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_educacion.")

    def connect_db_educacion_may_25(self, df):
        table_name = 'ecv_educacion_may25'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_educacion_may25.")

    def connect_db_educacion_quintiles(self, df):
        table_name = 'ecv_educacion_quintiles'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_educacion_quintiles.")

    def connect_db_transporte_medios(self, df):
        table_name = 'ecv_transporte_medios'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_transporte_medios.")

    def connect_db_transporte_desplazamiento(self, df):
        table_name = 'ecv_transporte_desplazamiento'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_transporte_desplazamiento.")

    def connect_db_transporte_tiempo_medio(self, df):
        table_name = 'ecv_transporte_tiempo_medio'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_transporte_tiempo_medio.")

    def connect_db_transporte_universitarios(self, df):
        table_name = 'ecv_transporte_universitarios'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_transporte_universitarios.")

    def connect_db_pobreza_impacto(self, df):
        table_name = 'ecv_pobreza_impacto'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_pobreza_impacto.")

    def connect_db_planes_impacto(self, df):
        table_name = 'ecv_planes_impacto'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_planes_impacto.")

    def connect_db_q_planes(self, df):
        table_name = 'ecv_q_planes'
        if self._should_update(df, table_name):
            self._replace_table(df, table_name)
        else:
            print("ℹ️ No hay cambios en los datos de ecv_q_planes.")
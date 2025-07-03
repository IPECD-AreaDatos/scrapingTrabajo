from sqlalchemy import create_engine
import pandas as pd

class connect_db:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")

    # ✅ Actualizar tabla eph_trabajo_tasas
    def actualizar_eph_trabajo_tasas(self, df):
        # Verificar cantidad de registros actuales
        df_bdd = pd.read_sql("SELECT * FROM eph_trabajo_tasas", con=self.engine)
        if len(df) > len(df_bdd):
            df.to_sql(name='eph_trabajo_tasas', con=self.engine, if_exists='replace', index=False)
            print("✅ Tabla eph_trabajo_tasas actualizada.")
        else:
            print("ℹ️ No hay cambios en los datos de eph_trabajo_tasas.")

    # ✅ Actualizar tabla eph_pobreza_tasas
    def actualizar_eph_pobreza(self, df):
        df_bdd = pd.read_sql("SELECT * FROM eph_pobreza_tasas", con=self.engine)
        if len(df) > len(df_bdd):
            df.to_sql(name='eph_pobreza_tasas', con=self.engine, if_exists='replace', index=False)
            print("✅ Tabla eph_pobreza_tasas actualizada.")
        else:
            print("ℹ️ No hay cambios en los datos de eph_pobreza_tasas.")

from sqlalchemy import create_engine
import pymysql
import pandas as pd

class load_database:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    
    def carga_datos(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="censo_estimado", con=engine, if_exists='replace', index=False)
        print("Base actualizada")

    def prepareFinalDataFrame(df_anual, df_trimestral):
        # Ordenar columnas de Anual y Trimestral para que tengan la misma estructura
        df_trimestral = df_trimestral[['Año', 'Trimestre', 'Frecuencia', 'Variable', 'Actividad', 'Valor']]
        df_anual = df_anual[['Año', 'Frecuencia', 'Variable', 'Actividad', 'Valor']]
        
        # Añadir columna Trimestre en df_anual para mantener compatibilidad
        df_anual['Trimestre'] = None

        # Unir los DataFrames
        df_final = pd.concat([df_anual, df_trimestral], axis=0, ignore_index=True)
        
        # Ordenar por Año y Trimestre
        df_final = df_final.sort_values(by=['Año', 'Trimestre'], ignore_index=True)
        
        return df_final

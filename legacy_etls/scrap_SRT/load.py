from pymysql import connect
from sqlalchemy import create_engine
import pandas as pd
import os

class ConexionBase:
    
    # definimos variables iniciales para la conexion
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

    # realizamos la conexion a la base de datos
    def conectar_bdd(self):

        self.conn = connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def probar(self):

        self.conectar_bdd()
        df_cobertura = pd.read_sql("SELECT * FROM cobertura_financiacion", self.conn)
        df_dicc = pd.read_sql("SELECT * FROM cobertura_financiacion_dicc", self.conn)

        print(df_cobertura)
        print(df_dicc)

        # Conservar solo una descripción por seccion (puedes cambiar 'first' por 'last' si es necesario)
        df_dicc = df_dicc.drop_duplicates(subset=['ciiu'], keep='first')

        # Hacer el merge para agregar la descripción de la sección
        df_cobertura = df_cobertura.merge(df_dicc[['ciiu', 'desc_ciiu']], on='ciiu', how='left')

        # Renombrar la columna agregada a 'seccion_descripcion' para mayor claridad
        df_cobertura.rename(columns={'desc_ciiu': 'ciiu_descripcion'}, inplace=True)

        print(df_cobertura)

    
    # realizamos la carga a la base de datos
    def carga_bdd(self,df, tabla):

        print("\n*****************************************************************************")
        print(f"****************inicio de la tabla {tabla}*******************")
        print("\n*****************************************************************************")

        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name=tabla, con=engine, if_exists='replace', index=False)
    
        print(f" == ACTUALIZACION == Nuevos datos en la base de {tabla}")

    def main(self, df, dicc_seccion, dicc_grupo, dicc_ciiu):

        self.conectar_bdd()
        self.carga_bdd(df, 'srt')
        self.carga_bdd(dicc_seccion, 'srt_seccion')
        self.carga_bdd(dicc_grupo, 'srt_grupo')
        self.carga_bdd(dicc_ciiu, 'srt_ciiu')
 
        # cerramos conexiones
        self.conn.commit()
        self.conn.close()
        self.cursor.close()

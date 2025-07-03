from sqlalchemy import create_engine
from pymysql import connect
import pandas as pd
from send_mail import EmailEmae

class Load():

    #Iniciamos atributos de las clases
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    #Objetivo: Conectarnos al DATALAKE ECONOMICO
    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()

    # ===================== ZONA DEL EMAE VALORES =====================#

    #Objetivo: verificar si existen mas datos en el DF que en la base de datos
    def verificar_emae_valores(self, df):
        df_bdd = pd.read_sql("SELECT fecha FROM emae", self.conn)
        fechas_existentes = set(df_bdd["fecha"])
        df_nuevos = df[~df["fecha"].isin(fechas_existentes)]
        return df_nuevos

    def load_emae_valores(self, df):
        df_nuevos = self.verificar_emae_valores(df)
        if not df_nuevos.empty:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df_nuevos.to_sql(name="emae", con=engine, if_exists='append', index=False)
            print("✅ EMAE VALORES actualizado con", len(df_nuevos), "registros nuevos.")
            return True
        else:
            print("ℹ️ No hay datos nuevos para EMAE VALORES.")
            return False


        # ===================== ZONA DEL EMAE VARIACIONES =====================#

    #Objetivo: verificar si existen mas datos en el DF que en la base de datos
    def verificar_emae_variaciones(self,df):

        #Buscamos los datos de los valores de EMAE ya almacenados
        df_bdd = pd.read_sql("SELECT * FROM emae_variaciones",self.conn)
        tamanio_bdd = len(df_bdd) #--> Tamano de la tabla del emae variaciones

        tamanio_df = len(df) #Tamano del df extraido.

        return tamanio_df , tamanio_bdd

    #Objetivo: Realizar la carga de EMAE VARIACIONES
    def load_emae_variaciones(self,df):

        #Buscamos los tamanos
        tamanio_df , tamanio_bdd = self.verificar_emae_variaciones(df)

        #Si es verdadero, realizar la carga de la diferencia
        if tamanio_df > tamanio_bdd:

            #Buscamos unicamente los datos nuevas de las variaciones
            df_tail = df.tail(tamanio_df - tamanio_bdd)
            
            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df_tail.to_sql(name="emae_variaciones", con=engine, if_exists='append', index=False)

            print("*************")
            print(" == ACTUALIZACION == ")
            print(" -- Se ha actualizado la base de datos de EMAE VARIACIONES.")
            print("*************")

            return True #Se actualizo la tabla, la bandera es positiva

        #Si es falso, descartar la carga
        else:
            print("*********")
            print("No existen datos nuevos para cargar de los -- VARIACIONES MENSUALES E INTERANUALES DE EMAE --")
            print("*********")

            return False #NO actualizo la tabla, la bandera es NEGATIVA

    """
    Este main tiene como proposito controlar las actualizaciones  y el envio del informe.
    Basicamente, se tendria que enviar el informe SOLO si las dos tablas se cargaron adecuadamente.
    Esto lo controlaremos con banderas.
    """
    def main_load(self,df_emae_valores,df_emae_variaciones):

        self.conectar_bdd()
    
        #Carga de los valores - Aca guardamos los valores de las banderas
        bandera_valores = self.load_emae_valores(df_emae_valores)
        bandera_variaciones = self.load_emae_variaciones(df_emae_variaciones)

        #Si ambas banderas son positivas, enviar correo. Sino, saltaran errores.
        if bandera_valores and bandera_variaciones:
            EmailEmae(self.host,self.user,self.password,self.database).main_correo()
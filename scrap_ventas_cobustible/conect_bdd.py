from pymysql import connect
from sqlalchemy import create_engine
import pandas as pd
import logging

class ConexionBaseDatos:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        self.engine = None
        
        # Configurar logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def conectar_bdd(self):
        try:
            # Establecer la conexión a la base de datos
            self.conn = connect(host=self.host, user=self.user, password=self.password, database=self.database)
            self.cursor = self.conn.cursor()
            self.logger.info("Conexión a la base de datos establecida con éxito.")
        except Exception as e:
            self.logger.error(f"Error al conectar con la base de datos: {e}")
            raise  # Volver a lanzar la excepción para que el proceso falle

    def verificar_carga(self, df):
        # Obtención del tamaño de la base de datos
        try:
            select_row_count_query = "SELECT COUNT(*) FROM combustible"
            self.cursor.execute(select_row_count_query)
            tamano_bdd = self.cursor.fetchone()[0]   
            tamano_df = len(df)  # Obtener tamaño del DataFrame

            return tamano_bdd, tamano_df
        except Exception as e:
            self.logger.error(f"Error al verificar el tamaño de la base de datos: {e}")
            raise

    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("*********************Inicio de la sección venta Combustible**********************")
        print("\n*****************************************************************************")
        
        # Obtención de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga(df)
        print(f"Tamaño actual en la base de datos: {tamano_bdd}")
        print(f"Tamaño del DataFrame: {tamano_df}")
        
        if tamano_df > tamano_bdd:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df_tail = df.tail(tamano_df - tamano_bdd)
            
            # Imprimir los datos nuevos (los que se van a cargar)
            print("************************ Datos nuevos que se van a cargar *************************")
            print(df_tail)
            print("*****************************************************************************")
            
            try:
                df_tail.to_sql(name='combustible', con=engine, if_exists='append', index=False)
                print("*************")
                print(f" == ACTUALIZACIÓN == Nuevos datos en la base de combustibles")
                print("*************")
                return True
            except Exception as e:
                print("Error durante la carga de datos:", e)
                return False
        else:
            print("*********")
            print("No existen datos nuevos de combustible")
            print("*********")
            return False

    def main(self, df):
        try:
            # Conectar a la base de datos
            self.conectar_bdd()
            
            # Ejecutar la carga de datos
            bandera = self.cargaBaseDatos(df)
            
            # Commit si la carga fue exitosa
            if bandera:
                self.conn.commit()
                self.logger.info("Transacción completada con éxito.")
            return bandera
        except Exception as e:
            self.logger.error(f"Error en el proceso principal: {e}")
            return False
        finally:
            # Cerrar la conexión y el cursor
            self.cerrar_conexion()

    def cerrar_conexion(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.logger.info("Conexión a la base de datos cerrada.")
        except Exception as e:
            self.logger.error(f"Error al cerrar la conexión a la base de datos: {e}")

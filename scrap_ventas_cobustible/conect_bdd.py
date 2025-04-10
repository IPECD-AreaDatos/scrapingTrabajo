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

    def obtener_ultima_fecha(self):
        try:
            # Obtener la última fecha registrada en la base de datos
            query = "SELECT MAX(fecha) FROM combustible"
            self.cursor.execute(query)
            ultima_fecha = self.cursor.fetchone()[0]  # Obtener el valor de la última fecha
            return ultima_fecha
        except Exception as e:
            self.logger.error(f"Error al obtener la última fecha: {e}")
            raise

    def verificar_nuevos_datos(self, df):
        # Verificar la última fecha registrada en la base de datos
        ultima_fecha_bdd = self.obtener_ultima_fecha()

        if ultima_fecha_bdd is None:  # Si no hay datos previos en la base de datos
            self.logger.info("No existen datos previos en la base de datos, cargando todos los datos.")
            return df  # Si no hay datos, cargar todo el DataFrame

        # Filtrar el DataFrame para obtener solo los datos posteriores a la última fecha registrada
        df['fecha'] = pd.to_datetime(df['fecha'])  # Asegurarse de que la columna 'fecha' esté en formato datetime
        df_nuevos = df[df['fecha'] > ultima_fecha_bdd]

        if not df_nuevos.empty:
            self.logger.info(f"Se encontraron {len(df_nuevos)} nuevos registros después de la última fecha {ultima_fecha_bdd}.")
        else:
            self.logger.info("No hay nuevos registros para cargar.")
        
        return df_nuevos

    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("*********************Inicio de la sección venta Combustible**********************")
        print("\n*****************************************************************************")
        
        print(df)
        # Verificar si existen datos nuevos
        df_nuevos = self.verificar_nuevos_datos(df)

        if not df_nuevos.empty:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            
            # Imprimir los datos nuevos (los que se van a cargar)
            print("************************ Datos nuevos que se van a cargar *************************")
            print(df_nuevos)
            print("*****************************************************************************")
            
            try:
                df_nuevos.to_sql(name='combustible', con=engine, if_exists='append', index=False)
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

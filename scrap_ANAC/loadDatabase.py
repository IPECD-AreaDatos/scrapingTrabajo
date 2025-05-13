from sqlalchemy import create_engine
import pymysql
import pandas as pd
import os

class load_database:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        """Conectar a la base de datos MySQL si no está ya conectada."""
        print(f"Intentando conectar a: host={self.host}, user={self.user}, password={self.password}, database={self.database}")
        if not self.conn or not self.cursor:
            try:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.cursor = self.conn.cursor()
                print("conec establecida")
            except pymysql.connector.Error as err:
                print(f"Error al conectar a la base de datos: {err}")
                return None
        return self

    def cerrar_conexion(self):
        """Cerrar la conexión a la base de datos."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None

    def load_data(self, df):
        """Cargar el DataFrame a la base de datos, reemplazando los datos existentes."""
        try:
            df = df.sort_values(by='fecha', ascending=True)
            ultima_fila = df.iloc[-1]
            ultima_fecha = ultima_fila['fecha']
            # Crear el motor de conexión a la base de datos
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")
            with engine.connect() as connection:
                # Sobrescribir los datos en la tabla
                df.to_sql(name="anac", con=connection, if_exists='replace', index=False)
            print("*****************************************")
            print(f" SE HA PRODUCIDO UNA CARGA DE DATOS DE ANAC PARA {ultima_fecha} ")
            print("*****************************************")
        except Exception as e:
            print(f"Error al cargar datos a la base de datos: {e}")

    def read_data_excel(self):
        """Leer datos desde la base de datos y limpiar los valores de la columna 'corrientes'."""
        # Conectar a la base de datos
        self.conectar_bdd()
        
        if not self.conn or not self.cursor:
            print("No se pudo establecer conexión con la base de datos.")
            return []

        try:
            table_name = 'anac'
            select_row_count_query = f"SELECT corrientes FROM {table_name}"
            self.cursor.execute(select_row_count_query)
            values = self.cursor.fetchall()
            
            # Limpiar los valores y convertir a float
            clean_values = list(
                filter(
                    lambda x: x is not None,
                    map(
                        lambda value: self._convertir_a_float(value[0]),
                        values
                    )
                )
            )
            return clean_values

        except pymysql.connect.Error as err:
            print(f"Error al leer datos de la base de datos: {err}")
            return []

        finally:
            # Cerrar la conexión
            self.cerrar_conexion()

    @staticmethod
    def _convertir_a_float(value):
        """Método auxiliar para limpiar y convertir un valor a float."""
        try:
            value_str = str(value).strip().replace(',', '.')
            return float(value_str)
        except ValueError:
            return None

# Ejemplo de uso
if __name__ == "__main__":
    # Parámetros de la base de datos (reemplaza con los valores correctos o usa variables de entorno)
    HOST = os.getenv("DB_HOST", "localhost")
    USER = os.getenv("DB_USER", "root")
    PASSWORD = os.getenv("DB_PASSWORD", "password")
    DATABASE = os.getenv("DB_NAME", "testdb")

    db_loader = load_database(HOST, USER, PASSWORD, DATABASE)
    
    # Ejemplo de DataFrame a cargar
    df_example = pd.DataFrame({
        'corrientes': [1, 2, 3],
        'cordoba': [4, 5, 6]
    })
    
    db_loader.load_data(df_example)
    clean_values = db_loader.read_data_excel()

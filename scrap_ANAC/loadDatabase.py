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
        if not self.conn or not self.cursor:
            try:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.cursor = self.conn.cursor()
                print("✓ Conexión a BD establecida")
            except pymysql.connector.Error as err:
                print(f"❌ Error al conectar a la base de datos: {err}")
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

    def obtener_ultima_fecha_bd(self):
        """Obtener la fecha más reciente en la base de datos."""
        self.conectar_bdd()
        
        if not self.conn or not self.cursor:
            print("No se pudo establecer conexión con la base de datos.")
            return None

        try:
            # Verificar si la tabla existe
            check_table_query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'anac'
            """
            self.cursor.execute(check_table_query, (self.database,))
            table_exists = self.cursor.fetchone()[0] > 0
            
            if not table_exists:
                print("La tabla 'anac' no existe. Primera carga.")
                return None
            
            # Obtener la última fecha
            select_max_date_query = "SELECT MAX(fecha) FROM anac"
            self.cursor.execute(select_max_date_query)
            result = self.cursor.fetchone()
            
            if result and result[0]:
                print(f"Última fecha en BD: {result[0]}")
                return result[0]
            else:
                print("No hay datos en la tabla.")
                return None

        except Exception as e:
            print(f"Error al consultar última fecha: {e}")
            return None

        finally:
            self.cerrar_conexion()

    def hay_datos_nuevos(self, df):
        """Verificar si el DataFrame contiene datos más recientes que la BD."""
        ultima_fecha_bd = self.obtener_ultima_fecha_bd()
        
        if ultima_fecha_bd is None:
            print("✓ Primera carga o tabla vacía. Proceder con carga completa.")
            return True
        
        # Obtener la fecha más reciente del DataFrame
        df_sorted = df.sort_values(by='fecha', ascending=True)
        ultima_fecha_df = df_sorted.iloc[-1]['fecha']
        
        print(f"Comparando fechas - BD: {ultima_fecha_bd} vs Excel: {ultima_fecha_df}")
        
        # Convertir a datetime si es necesario para comparar
        if isinstance(ultima_fecha_df, str):
            from datetime import datetime
            ultima_fecha_df = datetime.strptime(ultima_fecha_df, "%Y-%m-%d").date()
        
        if isinstance(ultima_fecha_bd, str):
            from datetime import datetime
            ultima_fecha_bd = datetime.strptime(ultima_fecha_bd, "%Y-%m-%d").date()
        
        if ultima_fecha_df > ultima_fecha_bd:
            print(f"✓ Datos nuevos encontrados. Última fecha Excel ({ultima_fecha_df}) > BD ({ultima_fecha_bd})")
            return True
        else:
            print(f"⚠ No hay datos nuevos. Última fecha Excel ({ultima_fecha_df}) <= BD ({ultima_fecha_bd})")
            return False

    def read_data_excel(self):
        """Leer el último valor y fecha de la columna 'corrientes' desde la base de datos."""
        # Conectar a la base de datos
        self.conectar_bdd()
        
        if not self.conn or not self.cursor:
            print("No se pudo establecer conexión con la base de datos.")
            return None, None

        try:
            table_name = 'anac'
            # Obtener el último registro (más reciente) con fecha y valor de corrientes
            select_last_query = f"SELECT corrientes, fecha FROM {table_name} ORDER BY fecha DESC LIMIT 1"
            self.cursor.execute(select_last_query)
            result = self.cursor.fetchone()
            
            if result:
                # Limpiar y convertir el último valor
                ultimo_valor = self._convertir_a_float(result[0])
                ultima_fecha = result[1]
                print(f"Último valor de Corrientes: {ultimo_valor} para fecha: {ultima_fecha}")
                return ultimo_valor, ultima_fecha
            else:
                print("No se encontraron datos en la tabla.")
                return None, None

        except pymysql.connect.Error as err:
            print(f"Error al leer datos de la base de datos: {err}")
            return None, None

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

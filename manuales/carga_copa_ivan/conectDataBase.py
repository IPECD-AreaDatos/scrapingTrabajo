import pymysql
from sqlalchemy import create_engine
import pandas as pd

class conectDataBase:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect_db(self) -> bool:
        """Establece la conexión con la base de datos MySQL."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print(f"Conexión exitosa a la base de datos '{self.database}'.")
            return True
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            return False

    def cerrar_conexion(self):
        """Cierra la conexión con la base de datos."""
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")

    def load_recursos_origen_nacional(self, df: pd.DataFrame):
        """
        Carga el DataFrame en la tabla `recursos_origen_nacional`,
        reemplazando los datos existentes (if_exists='replace').
        """
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}"
                f"?charset=utf8mb4"
            )
            with engine.connect() as connection:
                df.to_sql(
                    name="recursos_origen_nacional",
                    con=connection,
                    if_exists="replace",
                    index=False,
                )
            print("Datos cargados exitosamente en 'recursos_origen_nacional'.")
        except Exception as e:
            print(f"Error al cargar datos a la base de datos: {e}")

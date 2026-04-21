import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class ConnectDB:
    def __init__(self):
        # Cargar variables de entorno
        workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        dotenv_path = os.path.join(workspace_root, '.env')
        load_dotenv(dotenv_path=dotenv_path)

        # Usamos Base 2 (Postgres) segun DB_VERSION=2 en .env
        self.host = os.getenv("HOST_DBB2")
        self.port = os.getenv("PORT_DBB2", "5432")
        self.user = os.getenv("USER_DBB2")
        self.pwd = os.getenv("PASSWORD_DBB2")
        
        # El nombre de la base segun la inspeccion previa
        self.database = "datos_tablero"
        
        self.engine = None

    def connect(self):
        try:
            connection_string = f"postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string)
            with self.engine.connect() as conn:
                print(f"Conexión exitosa a la base de datos '{self.database}' (Postgres).")
                return True
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            return False

    def clear_table(self, table_name="copa_gastos"):
        """Limpia la tabla antes de la carga."""
        if not self.engine:
            return
        try:
            with self.engine.begin() as connection:
                print(f"Limpiando tabla {table_name}...")
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            print(f"Tabla {table_name} limpiada exitosamente.")
        except Exception as e:
            print(f"Error al limpiar la tabla: {e}")

    def load_data(self, df, table_name="copa_gastos"):
        """Carga el DataFrame en la tabla."""
        if not self.engine:
            return
        try:
            print(f"Cargando {len(df)} registros en {table_name}...")
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print("Carga finalizada con éxito.")
        except Exception as e:
            print(f"Error al cargar datos: {e}")

    def close(self):
        if self.engine:
            self.engine.dispose()

import pandas as pd
import pymysql
from sqlalchemy import create_engine
from numpy import trunc
import logging

class ConexionBaseDatos:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cursor = None
        self.conn = None

    def set_database(self, new_name):
        self.database = new_name

    def connect_db(self):
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False
            )
            self.cursor = self.conn.cursor()
            logging.info("✅ Conexión establecida con la base de datos")
        except Exception as e:
            raise RuntimeError(f"❌ Error de conexión: {e}")

    def close_connections(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    @property
    def engine(self):
        return create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")

    def read_sql(self, query):
        """Lee un SELECT y devuelve un DataFrame."""
        return pd.read_sql(query, self.engine)

    def truncate_table(self, table_name):
        """Vacía completamente la tabla."""
        self.cursor.execute(f"TRUNCATE {table_name}")
        self.conn.commit()
        logging.info(f"Tabla {table_name} truncada exitosamente.")

    def replace_table(self, table_name, df):
        """Trunca e inserta todo de nuevo."""
        self.truncate_table(table_name)
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        self.conn.commit()
        logging.info(f"Tabla {table_name} reemplazada exitosamente.")

    def insert_append(self, table_name, df):
        """Insertar datos sin borrar nada (puede duplicar)."""
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        self.conn.commit()
        logging.info(f"Datos insertados en {table_name} (modo append).")

    def replace_by_key(self, table_name, df, keys):
        """
        Reemplaza registros existentes basados en claves primarias.
        Útil para que no se dupliquen si ya existen.
        """
        cols = df.columns.tolist()
        values = df.values.tolist()
        placeholders = ", ".join(["%s"] * len(cols))
        columns = ", ".join(cols)

        query = f"REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.cursor.executemany(query, values)
        self.conn.commit()
        logging.info(f"REPLACE INTO completado en {table_name}.")

    def load_if_newer(self, df, table_name, date_column='fecha'):
        """
        Solo inserta datos más recientes que la fecha máxima de la tabla.
        Este método NO reemplaza fechas iguales.
        """
        query = f"SELECT MAX({date_column}) FROM {table_name}"
        self.cursor.execute(query)
        max_date = self.cursor.fetchone()[0]
        logging.info(f"Última fecha en base: {max_date}")

        df[date_column] = pd.to_datetime(df[date_column])
        nuevos = df[df[date_column] > pd.to_datetime(max_date)] if max_date else df

        if not nuevos.empty:
            self.insert_append(table_name, nuevos)
            logging.info(f"✅ {len(nuevos)} registros nuevos insertados.")
            return True
        else:
            logging.info("⚠️ No hay datos más recientes que cargar.")
            return False

    def upsert_all(self, df, table_name, key_columns):
        """
        Reemplaza o inserta todo el DataFrame usando REPLACE INTO.
        Ideal si querés garantizar que nunca quede duplicado.
        """
        self.replace_by_key(table_name, df, keys=key_columns)

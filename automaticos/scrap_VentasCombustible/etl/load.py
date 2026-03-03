"""
LOAD - Módulo de carga de datos VentasCombustible para PostgreSQL
"""
import os
import logging
import pandas as pd
import psycopg2
import pymysql
from sqlalchemy import create_engine, text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

class LoadVentasCombustible:
    """Carga datos de combustible a PostgreSQL y actualiza Google Sheets."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = version
        self.conn = None
        self.engine = None

    def _conectar(self):
        if not self.conn:
            try:
                if self.version == "1":  # MySQL
                    puerto = int(self.port) if self.port else 3306
                    self.conn = pymysql.connect(host=self.host, user=self.user, 
                                                password=self.password, database=self.database, port=puerto)
                    url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                else:  # PostgreSQL (v2)
                    puerto = int(self.port) if self.port else 5432
                    self.conn = psycopg2.connect(host=self.host, user=self.user, 
                                                 password=self.password, database=self.database, port=puerto)
                    url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                
                self.engine = create_engine(url)
                logger.info(f"[OK] Conectado a {'MySQL' if self.version=='1' else 'PostgreSQL'} (v{self.version})")
            except Exception as e:
                logger.error(f"[ERROR] Conexión fallida: {e}")
                raise

    def _get_schema(self):
        return "public." if self.version == "2" else ""

    def load(self, df: pd.DataFrame):
        """Carga datos simple: solo fecha, id_provincia, producto y cantidad."""
        try:
            self._conectar()
            schema = "public" if self.version == "2" else None
            
            # Aseguramos formatos para la comparación en memoria
            df['fecha'] = pd.to_datetime(df['fecha']).dt.date
            df['id_provincia'] = df['id_provincia'].astype(int)
            
            # Nombre de tabla para el SQL y para pandas
            table_name = "combustible"
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # 1. Crear tabla simple (sin PK, sin restricciones)
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    fecha DATE,
                    id_provincia INT,
                    producto VARCHAR(100),
                    cantidad DECIMAL(15, 2)
                );
            """
            with self.engine.begin() as conn:
                conn.execute(text(create_sql))

            # 2. Filtrado simple en memoria (Python hace el trabajo de evitar duplicados)
            query = f"SELECT fecha, id_provincia, producto FROM {full_table_name}"
            try:
                df_bdd = pd.read_sql(query, con=self.engine)
                df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha']).dt.date
                df_bdd['id_provincia'] = df_bdd['id_provincia'].astype(int)
                existentes = set(zip(df_bdd['fecha'], df_bdd['id_provincia'], df_bdd['producto']))
            except Exception:
                existentes = set()

            mask = df.apply(lambda x: (x['fecha'], x['id_provincia'], x['producto']) not in existentes, axis=1)
            df_nuevos = df[mask].copy()

            if df_nuevos.empty:
                logger.info("[LOAD] No hay registros nuevos para cargar.")
                return

            # 3. Carga simple
            df_nuevos.to_sql(table_name, self.engine, schema=schema, if_exists='append', index=False, chunksize=2000)
            logger.info(f"[LOAD] {len(df_nuevos)} registros cargados.")

            # 4. Actualizar Google Sheets
            ultima_fecha = df['fecha'].max()
            suma_total = df[df['fecha'] == ultima_fecha]['cantidad'].sum()
            self._update_sheets(suma_total, ultima_fecha)

        except Exception as e:
            logger.error(f"Error en carga de combustible: {e}")
            raise

    def _update_sheets(self, valor, fecha):
        """Actualiza la Fila 6 del Sheets institucional."""
        try:
            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
            creds = service_account.Credentials.from_service_account_info(
                key_dict, scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            service = build('sheets', 'v4', credentials=creds)

            # Formatear mes (ej: "sept-25")
            meses_es = {1:"ene", 2:"feb", 3:"mar", 4:"abr", 5:"may", 6:"jun",
                        7:"jul", 8:"ago", 9:"sept", 10:"oct", 11:"nov", 12:"dic"}
            tag_fecha = f"{meses_es[fecha.month]}-{str(fecha.year)[-2:]}"

            # Buscar columna en Fila 3
            res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="Datos!3:3").execute()
            headers = res.get("values", [[]])[0]
            
            col_idx = next((i for i, h in enumerate(headers) if h and tag_fecha in h.lower()), None)
            
            if col_idx is not None:
                letra = self._num_to_col(col_idx)
                rango = f"Datos!{letra}6" # FILA 6: Combustible Vendido
                val_str = str(valor).replace('.', ',') # Formato decimal para Sheets local
                
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=rango,
                    valueInputOption='USER_ENTERED', body={'values': [[val_str]]}
                ).execute()
                logger.info(f"[OK] Google Sheets (Combustible) actualizado en {rango}")
            else:
                logger.warning(f"No se encontró la columna para {tag_fecha} en el Sheets.")

        except Exception as e:
            logger.error(f"Error actualizando Sheets: {e}")

    def _num_to_col(self, n):
        res = ""
        while n >= 0:
            res = chr(n % 26 + 65) + res
            n = n // 26 - 1
        return res

    def close(self):
        if self.conn: self.conn.close()
        if self.engine: self.engine.dispose()
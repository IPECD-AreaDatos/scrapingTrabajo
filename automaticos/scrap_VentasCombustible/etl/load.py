"""
LOAD - Módulo de carga de datos VentasCombustible para PostgreSQL
"""
import os
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

class LoadVentasCombustible:
    """Carga datos de combustible a PostgreSQL y actualiza Google Sheets."""

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = 5432
        self.conn = None
        self.engine = None

    def _conectar(self):
        """Establece conexión con el servidor PostgreSQL."""
        if not self.conn:
            try:
                self.conn = psycopg2.connect(
                    host=self.host, user=self.user, 
                    password=self.password, database=self.database, port=self.port
                )
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                self.engine = create_engine(url)
                logger.info("[OK] Conexión a PostgreSQL establecida")
            except Exception as e:
                logger.error(f"[ERROR] No se pudo conectar a Postgres: {e}")
                raise

    def load(self, df: pd.DataFrame):
        """Carga datos a la base de datos y dispara la actualización del Sheets."""
        try:
            self._conectar()
            cursor = self.conn.cursor()

            # 1. Crear tabla si no existe (Esquema Postgres)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.combustible (
                    fecha DATE,
                    provincia INT,
                    producto VARCHAR(100),
                    cantidad NUMERIC(15, 2),
                    PRIMARY KEY (fecha, provincia, producto)
                );
            """)
            self.conn.commit()

            # 2. Verificar datos nuevos (por fecha)
            cursor.execute("SELECT MAX(fecha) FROM public.combustible")
            ultima_fecha_bd = cursor.fetchone()[0]

            df['fecha'] = pd.to_datetime(df['fecha']).dt.date
            if ultima_fecha_bd:
                df_nuevos = df[df['fecha'] > ultima_fecha_bd]
            else:
                df_nuevos = df

            if df_nuevos.empty:
                logger.info("[LOAD] No hay registros nuevos de combustible para cargar.")
                return

            # 3. Carga a PostgreSQL
            df_nuevos.to_sql('combustible', self.engine, schema='public', if_exists='append', index=False)
            logger.info(f"[LOAD] {len(df_nuevos)} registros cargados en PostgreSQL.")

            # 4. Actualizar Google Sheets (Usando la suma de la última fecha)
            ultima_fecha = df['fecha'].max()
            suma_total = df[df['fecha'] == ultima_fecha]['cantidad'].sum()
            self._update_sheets(suma_total, ultima_fecha)

        except Exception as e:
            if self.conn: self.conn.rollback()
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
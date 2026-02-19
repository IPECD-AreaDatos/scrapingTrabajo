"""
LOAD - Módulo de carga de datos DNRPA
Responsabilidad: Cargar datos a PostgreSQL y actualizar Google Sheets (Fila 7 y 8)
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

class LoadDNRPA:
    def __init__(self, host, user, password, database):
        self.host, self.user, self.password, self.database = host, user, password, database
        self.port = 5432
        self.conn = None
        self.engine = None

    def _conectar(self):
        if not self.conn:
            # Conexión nativa Postgres
            self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=self.port)
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(url)

    def load(self, df: pd.DataFrame):
        try:
            self._conectar()
            cursor = self.conn.cursor()

            # 1. Crear tabla si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.dnrpa (
                    fecha DATE,
                    id_provincia_indec INT,
                    id_vehiculo INT,
                    cantidad INT,
                    PRIMARY KEY (fecha, id_provincia_indec, id_vehiculo)
                );
            """)
            self.conn.commit()

            # 2. Verificar si hay cambios en el último año
            ultimo_anio = int(df['fecha'].dt.year.max())
            df_anio = df[df['fecha'].dt.year == ultimo_anio]
            
            # En Postgres usamos EXTRACT
            cursor.execute("SELECT COUNT(*) FROM public.dnrpa WHERE EXTRACT(YEAR FROM fecha) = %s", (ultimo_anio,))
            count_bd = cursor.fetchone()[0]

            if count_bd == len(df_anio):
                logger.info(f"[LOAD] DNRPA: Sin datos nuevos para el año {ultimo_anio}.")
                return

            # 3. Recarga: Borrar e Insertar año actual
            logger.info(f"[LOAD] Recargando año {ultimo_anio} en Postgres...")
            cursor.execute("DELETE FROM public.dnrpa WHERE EXTRACT(YEAR FROM fecha) = %s", (ultimo_anio,))
            self.conn.commit()

            df_anio.to_sql('dnrpa', self.engine, schema='public', if_exists='append', index=False)
            
            # 4. Actualizar Google Sheets (Filas 7 y 8)
            self._update_sheets(df_anio)

        except Exception as e:
            if self.conn: self.conn.rollback()
            logger.error(f"Error en Load DNRPA: {e}")
            raise

    def _update_sheets(self, df):
        """Actualiza Patentamiento de Autos (Fila 7) y Motos (Fila 8) para Corrientes (18)"""
        try:
            # Filtrar Corrientes y última fecha
            ultima_fecha = df['fecha'].max()
            df_corr = df[(df['id_provincia_indec'] == 18) & (df['fecha'] == ultima_fecha)]
            
            # id_vehiculo: 1 -> Autos, 2 -> Motos
            val_autos = df_corr[df_corr['id_vehiculo'] == 1]['cantidad'].sum()
            val_motos = df_corr[df_corr['id_vehiculo'] == 2]['cantidad'].sum()

            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=['https://www.googleapis.com/auth/spreadsheets'])
            service = build('sheets', 'v4', credentials=creds)

            meses_es = {1:"ene", 2:"feb", 3:"mar", 4:"abr", 5:"may", 6:"jun", 7:"jul", 8:"ago", 9:"sept", 10:"oct", 11:"nov", 12:"dic"}
            tag_fecha = f"{meses_es[ultima_fecha.month]}-{str(ultima_fecha.year)[-2:]}"

            # Buscar columna en Fila 3
            res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="Datos!3:3").execute()
            headers = res.get("values", [[]])[0]
            col_idx = next((i for i, h in enumerate(headers) if h and tag_fecha in h.lower()), None)
            
            if col_idx is not None:
                letra = self._num_to_col(col_idx)
                # Fila 7: Autos, Fila 8: Motos
                batch_data = [
                    {'range': f"Datos!{letra}7", 'values': [[str(val_autos)]]},
                    {'range': f"Datos!{letra}8", 'values': [[str(val_motos)]]}
                ]
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SPREADSHEET_ID, 
                    body={'valueInputOption': 'USER_ENTERED', 'data': batch_data}
                ).execute()
                logger.info(f"[OK] Sheets actualizado para {tag_fecha}: Autos ({val_autos}), Motos ({val_motos})")
        except Exception as e:
            logger.error(f"Error actualizando Sheets DNRPA: {e}")

    def _num_to_col(self, n):
        res = ""
        while n >= 0:
            res = chr(n % 26 + 65) + res
            n = n // 26 - 1
        return res

    def close(self):
        if self.conn: self.conn.close()
        if self.engine: self.engine.dispose()
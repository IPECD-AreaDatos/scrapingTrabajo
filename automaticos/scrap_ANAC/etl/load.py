"""
LOAD - Módulo de carga de datos ANAC
Responsabilidad: Cargar datos a PostgreSQL y actualizar Google Sheets
"""
import os
import logging
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

class LoadANAC:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = 5432
        self.conn = None
        self.cursor = None
        self.engine = None

    def conectar_bdd(self):
        """Conecta a la base de datos PostgreSQL"""
        if not self.conn:
            try:
                # Conexión nativa para Postgres
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    port=self.port
                )
                self.cursor = self.conn.cursor()
                
                # Engine para SQLAlchemy adaptado a Postgres
                connection_string = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                self.engine = create_engine(connection_string)
                
                logger.info("[OK] Conexión a PostgreSQL establecida")
            except Exception as err:
                logger.error(f"[ERROR] No se pudo conectar a Postgres: {err}")
                raise
        return self

    def cerrar_conexion(self):
        """Cierra las conexiones"""
        try:
            if self.cursor: self.cursor.close()
            if self.conn: self.conn.close()
            if self.engine: self.engine.dispose()
            self.conn = self.cursor = self.engine = None
            logger.info("Conexión a BD cerrada")
        except Exception as e:
            logger.warning(f"Error al cerrar conexión: {e}")

    def _obtener_ultima_fecha_bd(self):
        """Obtiene la fecha más reciente en la BD"""
        self.conectar_bdd()
        try:
            # En Postgres el esquema suele ser 'public' a menos que hayas creado 'datalake_economico' específicamente
            query = "SELECT MAX(fecha) FROM public.anac"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result and result[0]:
                return pd.to_datetime(result[0]).date()
            return None
        except Exception as e:
            logger.info(f"Tabla no encontrada o vacía: {e}")
            return None

    def hay_datos_nuevos(self, df):
        """Compara fechas entre Excel y BD"""
        ultima_fecha_bd = self._obtener_ultima_fecha_bd()
        if ultima_fecha_bd is None: return True

        ultima_fecha_df = pd.to_datetime(df['fecha'].max()).date()
        logger.info(f"Comparando - BD: {ultima_fecha_bd} vs Excel: {ultima_fecha_df}")
        return ultima_fecha_df > ultima_fecha_bd

    def load_to_database(self, df):
        """Carga el DataFrame a Postgres con limpieza de duplicados"""
        logger.info("=== VERIFICACIÓN DE DATOS ANTES DE CARGAR ===")
        filas_corrientes = df[df['aeropuerto'].str.contains('corrientes', case=False, na=False)]
        logger.info(f"Total filas: {len(df)} | Filas Corrientes: {len(filas_corrientes)}")

        try:
            self.conectar_bdd()
            df['fecha'] = pd.to_datetime(df['fecha']).dt.date
            primera_fecha = df['fecha'].min()
            ultima_fecha = df['fecha'].max()

            # 1. Crear tabla si no existe (Sintaxis Postgres)
            create_table_query = """
                CREATE TABLE IF NOT EXISTS public.anac (
                    fecha DATE,
                    aeropuerto VARCHAR(100),
                    cantidad NUMERIC(15, 2),
                    PRIMARY KEY (fecha, aeropuerto)
                );
            """
            self.cursor.execute(create_table_query)
            self.conn.commit()

            # 2. Eliminar datos existentes en el rango para evitar conflictos de Primary Key
            delete_query = "DELETE FROM public.anac WHERE fecha >= %s AND fecha <= %s"
            self.cursor.execute(delete_query, (primera_fecha, ultima_fecha))
            self.conn.commit()

            # 3. Carga masiva eficiente
            df.to_sql('anac', self.engine, schema='public', if_exists='append', index=False)
            logger.info(f"[OK] Carga finalizada: {len(df)} registros insertados.")

        except Exception as e:
            if self.conn: self.conn.rollback()
            logger.error(f"Error en carga a BD: {e}")
            raise

    def obtener_ultimo_valor_corrientes(self):
        """Recupera el dato más reciente de Corrientes"""
        try:
            self.conectar_bdd()
            query = """
                SELECT cantidad, fecha FROM public.anac 
                WHERE aeropuerto ILIKE '%corrientes%' 
                ORDER BY fecha DESC LIMIT 1
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                return float(result[0]), result[1]
            return None, None
        except Exception as e:
            logger.error(f"Error al obtener valor de Corrientes: {e}")
            return None, None

    def load_to_sheets(self, ultimo_valor, ultima_fecha):
        try:
            if ultimo_valor is None: return
            
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()

            # Formato exacto: dic-25
            meses_es = {1:"ene", 2:"feb", 3:"mar", 4:"abr", 5:"may", 6:"jun",
                        7:"jul", 8:"ago", 9:"sept", 10:"oct", 11:"nov", 12:"dic"}
            fecha_sheets = f"{meses_es[ultima_fecha.month]}-{str(ultima_fecha.year)[-2:]}"
            logger.info(f"Buscando columna para: {fecha_sheets}")

            # Leemos la fila 3 para ubicar la columna
            res = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Datos!3:3").execute()
            headers = res.get("values", [[]])[0]
            
            # Búsqueda exacta para evitar confusiones
            col_idx = None
            for i, h in enumerate(headers):
                if h and str(h).strip().lower() == fecha_sheets.lower():
                    col_idx = i
                    break
            
            if col_idx is None:
                logger.error(f"No se encontró la columna '{fecha_sheets}' en la fila 3")
                return

            letra_col = self.num_to_col(col_idx)
            rango_celda = f"Datos!{letra_col}10"
            
            # CAMBIO: Usamos USER_ENTERED y forzamos el formato de número
            valor_final = str(ultimo_valor).replace('.', ',') # Para que el Excel lo tome como número
            body = {'values': [[valor_final]]}
            
            logger.info(f"Intentando escribir {valor_final} en {rango_celda}...")
            
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, 
                range=rango_celda,
                valueInputOption='USER_ENTERED', # Cambio aquí
                body=body
            ).execute()
            
            logger.info(f"[OK] Sheets actualizado correctamente en {rango_celda}")

        except Exception as e:
            logger.error(f"Error crítico en Sheets: {e}")

    def num_to_col(self, n):
        res = ""
        while n >= 0:
            res = chr(n % 26 + 65) + res
            n = n // 26 - 1
        return res
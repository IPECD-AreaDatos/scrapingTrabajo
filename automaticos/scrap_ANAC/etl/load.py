"""
LOAD - Módulo de carga de datos ANAC
Responsabilidad: Cargar datos a PostgreSQL y actualizar Google Sheets
"""
import os
import logging
import pandas as pd
import psycopg2
import pymysql  # Necesario para la base vieja si es MySQL
from sqlalchemy import create_engine, text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

class LoadANAC:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.conn = None
        self.engine = None

    def conectar_bdd(self):
        """Detecta el motor y conecta a la base correspondiente"""
        if not self.conn:
            try:
                if self.version == "1":
                    # --- LÓGICA PARA MYSQL (Base Vieja) ---
                    # MySQL usa puerto 3306 por defecto si no se especifica
                    puerto_mysql = int(self.port) if self.port else 3306
                    self.conn = pymysql.connect(
                        host=self.host,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        port=puerto_mysql
                    )
                    conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto_mysql}/{self.database}"
                    logger.info(f"[OK] Conectado a MySQL (v1) en {self.host}")
                else:
                    # --- LÓGICA PARA POSTGRESQL (Base Nueva) ---
                    puerto_pg = self.port if self.port else 5432
                    self.conn = psycopg2.connect(
                        host=self.host,
                        user=self.user,
                        password=self.password,
                        database=self.database,
                        port=puerto_pg
                    )
                    conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto_pg}/{self.database}"
                    logger.info(f"[OK] Conectado a PostgreSQL (v2) en {self.host}")

                self.engine = create_engine(conn_str)
                self.cursor = self.conn.cursor()
            except Exception as err:
                logger.error(f"[ERROR] No se pudo conectar a la base v{self.version}: {err}")
                raise
        return self
    
    def _get_schema_prefix(self):
        """Retorna el prefijo de esquema solo si es Postgres."""
        return "public." if self.version == "2" else ""

    def load_to_database(self, df):
        self.conectar_bdd()
        # Aseguramos nombres de columnas correctos antes de insertar
        df = df[['fecha', 'aeropuerto', 'cantidad']]
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        tabla = f"{self._get_schema_prefix()}anac"
        
        try:
            # 1. Borrado eficiente
            fecha_min = df['fecha'].min()
            fecha_max = df['fecha'].max()
            
            # Usar sentencias seguras con parámetros
            sql_delete = text(f"DELETE FROM {tabla} WHERE fecha BETWEEN :fmin AND :fmax")
            with self.engine.begin() as conn:
                conn.execute(sql_delete, {"fmin": fecha_min, "fmax": fecha_max})
                
                # 2. Inserción limpia
                df.to_sql(name='anac', con=conn, if_exists='append', index=False, method='multi')
            
            logger.info(f"[LOAD] v{self.version} OK: {len(df)} filas cargadas.")
        except Exception as e:
            logger.error(f"[LOAD ERROR] v{self.version}: {e}")
            raise

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

    def obtener_ultimo_valor_corrientes(self):
        self.conectar_bdd()
        # Si es Postgres (v2), usamos public.anac
        tabla = f"{self._get_schema_prefix()}anac"
        like_op = "ILIKE" if self.version == "2" else "LIKE"
        
        # Filtramos también por la cantidad > 0 para evitar errores
        query = f"""
            SELECT cantidad, fecha FROM {tabla} 
            WHERE aeropuerto {like_op} '%corrientes%' 
            AND cantidad > 0
            ORDER BY fecha DESC LIMIT 1
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return (float(result[0]), result[1]) if result else (None, None)

    def load_to_sheets(self, ultimo_valor, ultima_fecha):
        try:
            if ultimo_valor is None: return
            
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
            service = build('sheets', 'v4', credentials=creds)
            
            # MESES EN ESPAÑOL (Asegúrate que coincida con tu fila 3 del Sheets)
            meses_es = {1:"ene", 2:"feb", 3:"mar", 4:"abr", 5:"may", 6:"jun",
                        7:"jul", 8:"ago", 9:"sept", 10:"oct", 11:"nov", 12:"dic"}
            
            # Formato esperado: "ene-26"
            fecha_buscada = f"{meses_es[ultima_fecha.month]}-{str(ultima_fecha.year)[-2:]}"
            logger.info(f"Buscando columna para: {fecha_buscada}")

            res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="Datos!3:3").execute()
            headers = res.get("values", [[]])[0]
            
            col_idx = None
            for i, h in enumerate(headers):
                if h and str(h).strip().lower() == fecha_buscada.lower():
                    col_idx = i
                    break
            
            if col_idx is None:
                # Si no lo encuentra, tiramos un warning pero no matamos el proceso
                logger.warning(f"[WARN] No se encontró la columna '{fecha_buscada}' en la fila 3 de Sheets.")
                return

            letra_col = self.num_to_col(col_idx)
            rango_celda = f"Datos!{letra_col}10" # Celda de Corrientes
            
            valor_formateado = float(ultimo_valor)
            body = {'values': [[valor_formateado]]}
            
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, 
                range=rango_celda,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"[OK] Sheets actualizado en {rango_celda} con valor {valor_formateado}")

        except Exception as e:
            logger.error(f"Error en Sheets: {e}")

    def num_to_col(self, n):
        res = ""
        while n >= 0:
            res = chr(n % 26 + 65) + res
            n = n // 26 - 1
        return res
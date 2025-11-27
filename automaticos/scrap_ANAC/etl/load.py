"""
LOAD - Módulo de carga de datos ANAC
Responsabilidad: Cargar datos a MySQL y actualizar Google Sheets
"""
from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import os
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from json import loads
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

class LoadANAC:
    """Clase para cargar datos de ANAC a BD y Google Sheets"""
    
    def __init__(self, host, user, password, database):
        """
        Inicializa el cargador
        
        Args:
            host: Host de la base de datos
            user: Usuario de la base de datos
            password: Contraseña de la base de datos
            database: Nombre de la base de datos
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        self.engine = None

    def hay_datos_nuevos(self, df):
        """
        Verifica si el DataFrame contiene datos más recientes que la BD
        
        Args:
            df: DataFrame a verificar
            
        Returns:
            bool: True si hay datos nuevos, False en caso contrario
        """
        ultima_fecha_bd = self._obtener_ultima_fecha_bd()
        
        if ultima_fecha_bd is None:
            logger.info("[OK] Primera carga o tabla vacía. Proceder con carga completa.")
            return True
        
        # Obtener la fecha más reciente del DataFrame
        df_sorted = df.sort_values(by='fecha', ascending=True)
        ultima_fecha_df = df_sorted.iloc[-1]['fecha']
        
        logger.info(f"Comparando fechas - BD: {ultima_fecha_bd} vs Excel: {ultima_fecha_df}")
        
        # Convertir a datetime si es necesario
        if isinstance(ultima_fecha_df, str):
            from datetime import datetime
            ultima_fecha_df = datetime.strptime(ultima_fecha_df, "%Y-%m-%d").date()
        
        if isinstance(ultima_fecha_bd, str):
            from datetime import datetime
            ultima_fecha_bd = datetime.strptime(ultima_fecha_bd, "%Y-%m-%d").date()
        
        if ultima_fecha_df > ultima_fecha_bd:
            logger.info(f"[OK] Datos nuevos encontrados. Última fecha Excel ({ultima_fecha_df}) > BD ({ultima_fecha_bd})")
            return True
        else:
            logger.info(f"[WARNING] No hay datos nuevos. Última fecha Excel ({ultima_fecha_df}) <= BD ({ultima_fecha_bd})")
            return False

    def conectar_bdd(self):
        """Conecta a la base de datos MySQL"""
        if not self.conn or not self.cursor:
            try:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                self.cursor = self.conn.cursor()
                
                # Crear engine para SQLAlchemy
                connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}"
                self.engine = create_engine(connection_string)
                
                logger.info("[OK] Conexión a BD establecida")
            except Exception as err:
                logger.error(f"[ERROR] Error al conectar a la base de datos: {err}")
                raise
        return self

    def cerrar_conexion(self):
        """Cierra la conexión a la base de datos"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            if self.engine:
                self.engine.dispose()
            self.conn = None
            self.cursor = None
            self.engine = None
            logger.info("Conexión a BD cerrada")
        except Exception as e:
            logger.warning(f"Error al cerrar conexión: {e}")

    def load_to_database(self, df):
        """
        Carga el DataFrame a la base de datos MySQL usando UPSERT
        Solo actualiza/inserta los datos del DataFrame, manteniendo datos históricos anteriores
        
        Args:
            df: DataFrame a cargar
        """
        try:
            df = df.sort_values(by='fecha', ascending=True)
            ultima_fila = df.iloc[-1]
            ultima_fecha = ultima_fila['fecha']
            primera_fecha = df.iloc[0]['fecha']
            
            if not self.engine:
                self.conectar_bdd()
            
            with self.engine.connect() as connection:
                # Verificar si la tabla existe
                check_table_query = text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = :database AND table_name = 'anac'
                """)
                result = connection.execute(check_table_query, {"database": self.database})
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    # Si la tabla no existe, crearla con replace
                    logger.info("Tabla 'anac' no existe. Creando tabla con todos los datos...")
                    df.to_sql(name="anac", con=connection, if_exists='replace', index=False)
                    connection.commit()
                else:
                    # Si la tabla existe, hacer UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)
                    logger.info(f"Actualizando datos desde {primera_fecha} hasta {ultima_fecha} (manteniendo datos históricos anteriores)")
                    
                    # Eliminar solo los registros en el rango de fechas del DataFrame
                    # para evitar duplicados antes de insertar
                    delete_query = text("""
                        DELETE FROM anac 
                        WHERE fecha >= :fecha_inicio AND fecha <= :fecha_fin
                    """)
                    connection.execute(delete_query, {
                        "fecha_inicio": primera_fecha,
                        "fecha_fin": ultima_fecha
                    })
                    connection.commit()
                    
                    # Insertar los nuevos datos
                    df.to_sql(name="anac", con=connection, if_exists='append', index=False)
                    connection.commit()
                    
                    logger.info(f"[OK] Datos actualizados: {len(df)} registros desde {primera_fecha} hasta {ultima_fecha}")
            
            logger.info("=" * 50)
            logger.info(f"[OK] SE HA PRODUCIDO UNA CARGA DE DATOS DE ANAC PARA {ultima_fecha}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error al cargar datos a la base de datos: {e}")
            raise

    def load_to_sheets(self, ultimo_valor, ultima_fecha):
        """
        Actualiza Google Sheets con el último valor
        
        Args:
            ultimo_valor: Último valor de Corrientes
            ultima_fecha: Fecha del último dato
        """
        try:
            if ultimo_valor is None or ultima_fecha is None:
                logger.warning("No se pudieron obtener datos para actualizar Sheets")
                return
            
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
            
            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            
            # Buscar última columna con datos
            fila_pasajeros = 10
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f'Datos!A{fila_pasajeros}:CZ{fila_pasajeros}'
            ).execute()
            
            row_values = result.get('values', [[]])[0] if result.get('values') else []
            ultima_columna_con_datos = 0
            for i, valor in enumerate(row_values):
                if valor and str(valor).strip():
                    ultima_columna_con_datos = i + 1
            
            # Leer headers de fecha
            result_headers = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f'Datos!A3:CZ3'
            ).execute()
            
            header_values = result_headers.get('values', [[]])[0] if result_headers.get('values') else []
            
            # Convertir número a letra de columna
            def numero_a_columna(num):
                resultado = ""
                while num > 0:
                    num -= 1
                    resultado = chr(65 + num % 26) + resultado
                    num //= 26
                return resultado
            
            # Verificar si ya existe el período
            from datetime import datetime
            if isinstance(ultima_fecha, str):
                fecha_obj = datetime.strptime(ultima_fecha, "%Y-%m-%d")
            else:
                fecha_obj = ultima_fecha
            periodo_dato = fecha_obj.strftime("%b-%y").lower()
            
            periodo_encontrado = False
            for i, header in enumerate(header_values):
                if header and periodo_dato in str(header).lower():
                    periodo_encontrado = True
                    proxima_columna = i + 1
                    logger.info(f"Período {periodo_dato} ya existe en columna {numero_a_columna(proxima_columna)}")
                    break
            
            if not periodo_encontrado:
                proxima_columna = ultima_columna_con_datos + 1
                logger.info(f"Creando nueva columna para período {periodo_dato}")
            
            letra_columna = numero_a_columna(proxima_columna)
            rango_destino = f'Datos!{letra_columna}{fila_pasajeros}'
            
            # Escribir valor
            body = {'values': [[ultimo_valor]]}
            request = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=rango_destino,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"[OK] {request.get('updatedCells')} celda(s) actualizada(s) en {rango_destino}!")
            
        except Exception as e:
            logger.error(f"Error al actualizar Google Sheets: {e}")
            raise

    def obtener_ultimo_valor_corrientes(self):
        """
        Obtiene el último valor de Corrientes desde la BD
        
        Returns:
            tuple: (ultimo_valor, ultima_fecha) o (None, None) si no hay datos
        """
        self.conectar_bdd()
        
        if not self.conn or not self.cursor:
            logger.error("No se pudo establecer conexión con la base de datos")
            return None, None
        
        try:
            select_last_query = "SELECT corrientes, fecha FROM anac ORDER BY fecha DESC LIMIT 1"
            self.cursor.execute(select_last_query)
            result = self.cursor.fetchone()
            
            if result:
                ultimo_valor = self._convertir_a_float(result[0])
                ultima_fecha = result[1]
                logger.info(f"Último valor de Corrientes: {ultimo_valor} para fecha: {ultima_fecha}")
                return ultimo_valor, ultima_fecha
            else:
                logger.warning("No se encontraron datos en la tabla")
                return None, None
                
        except Exception as e:
            logger.error(f"Error al leer datos de la base de datos: {e}")
            return None, None

    def _obtener_ultima_fecha_bd(self):
        """Obtiene la fecha más reciente en la base de datos"""
        self.conectar_bdd()
        
        if not self.conn or not self.cursor:
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
                logger.info("La tabla 'anac' no existe. Primera carga.")
                return None
            
            # Obtener la última fecha
            select_max_date_query = "SELECT MAX(fecha) FROM anac"
            self.cursor.execute(select_max_date_query)
            result = self.cursor.fetchone()
            
            if result and result[0]:
                logger.info(f"Última fecha en BD: {result[0]}")
                return result[0]
            else:
                logger.info("No hay datos en la tabla")
                return None
                
        except Exception as e:
            logger.error(f"Error al consultar última fecha: {e}")
            return None

    @staticmethod
    def _convertir_a_float(value):
        """Convierte un valor a float"""
        try:
            value_str = str(value).strip().replace(',', '.')
            return float(value_str)
        except ValueError:
            return None


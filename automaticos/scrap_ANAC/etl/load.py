"""
LOAD - Módulo de carga de datos ANAC
Responsabilidad: Cargar datos a MySQL y actualizar Google Sheets
"""
from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
from datetime import datetime, date 
import time
from dateutil import parser
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
        """
        ultima_fecha_bd = self._obtener_ultima_fecha_bd()

        # Si no hay datos en BD → es una carga nueva
        if ultima_fecha_bd is None:
            logger.info("[OK] Primera carga o tabla vacía. Proceder con carga completa.")
            return True

        # Obtener la última fecha del Excel (siempre será Timestamp si viene del DF)
        ultima_fecha_df = df['fecha'].max()

        # 💥 Convertir todas las fechas SIEMPRE a Timestamp
        ultima_fecha_df = pd.to_datetime(ultima_fecha_df)
        ultima_fecha_bd = pd.to_datetime(ultima_fecha_bd)

        logger.info(f"Comparando fechas - BD: {ultima_fecha_bd} vs Excel: {ultima_fecha_df}")

        # Comparar
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
        Carga el DataFrame a la base de datos MySQL usando UPSERT.
        Adaptada para esquema vertical (fecha, aeropuerto, cantidad).
        """

        # AGREGAR LOGGING ANTES DE CARGAR
        logger.info("=== VERIFICACIÓN DE DATOS ANTES DE CARGAR ===")
        logger.info(f"Total de filas en DataFrame: {len(df)}")
        
        # Ver qué valores únicos de aeropuerto hay en el DataFrame
        valores_unicos = df['aeropuerto'].unique()
        logger.info(f"Valores únicos de 'aeropuerto' en DataFrame: {valores_unicos}")
        
        # Contar cuántas filas tienen "corrientes"
        filas_corrientes = df[df['aeropuerto'].str.contains('corrientes', case=False, na=False)]
        logger.info(f"Filas con 'corrientes': {len(filas_corrientes)}")
        
        if len(filas_corrientes) > 0:
            logger.info(f"Ejemplo de datos de corrientes: {filas_corrientes.head()}")
        else:
            logger.warning("ADVERTENCIA: DataFrame NO contiene 'corrientes'")
            
            # Ver qué sí contiene
            conteo_aeropuertos = df['aeropuerto'].value_counts()
            logger.info(f"Distribución de aeropuertos en DataFrame:\n{conteo_aeropuertos}")
        
        logger.info("=== FIN VERIFICACIÓN ===")

        try:
            # Asegurar que 'fecha' es datetime
            df['fecha'] = pd.to_datetime(df['fecha'])
            
            df = df.sort_values(by='fecha', ascending=True)
            ultima_fecha = df.iloc[-1]['fecha']
            primera_fecha = df.iloc[0]['fecha']

            # ------------------------------
            # Asegurar conexión USANDO PYMYSQL DIRECTAMENTE
            # ------------------------------
            self.conectar_bdd()  # Asegurar conexión pymysql
            
            # Verificar si la tabla existe
            check_table_query = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = 'anac'
            """
            self.cursor.execute(check_table_query, (self.database,))
            table_exists = self.cursor.fetchone()[0] > 0

            # ---------------------------------------------------
            # PRIMERA CARGA → CREAR TABLA SI NO EXISTE
            # ---------------------------------------------------
            if not table_exists:
                logger.info("Tabla 'anac' no existe. Creando tabla...")
                
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS datalake_economico.anac (
                        fecha DATE,
                        aeropuerto VARCHAR(100),
                        cantidad DECIMAL(15, 2),
                        PRIMARY KEY (fecha, aeropuerto),
                        INDEX idx_fecha (fecha),
                        INDEX idx_aeropuerto (aeropuerto)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                self.cursor.execute(create_table_query)
                self.conn.commit()
                logger.info("Tabla 'anac' creada exitosamente")

            # ---------------------------------------------------
            # VERIFICAR DATOS EXISTENTES
            # ---------------------------------------------------
            logger.info(f"Actualizando datos desde {primera_fecha} hasta {ultima_fecha}")
            
            # Primero verificar si hay datos para eliminar
            count_query = """
                SELECT COUNT(*) 
                FROM datalake_economico.anac 
                WHERE fecha >= %s AND fecha <= %s
            """
            self.cursor.execute(count_query, (primera_fecha, ultima_fecha))
            registros_a_eliminar = self.cursor.fetchone()[0]
            logger.info(f"Registros a eliminar en rango: {registros_a_eliminar}")

            if registros_a_eliminar > 0:
                delete_query = """
                    DELETE FROM datalake_economico.anac
                    WHERE fecha >= %s AND fecha <= %s
                """
                self.cursor.execute(delete_query, (primera_fecha, ultima_fecha))
                self.conn.commit()
                logger.info(f"Registros eliminados: {registros_a_eliminar}")
            else:
                logger.info("No hay registros previos en el rango, procediendo con inserción")

            # ---------------------------------------------------
            # INSERTAR NUEVOS DATOS USANDO PYMYSQL DIRECTAMENTE
            # ---------------------------------------------------
            # Preparar datos para inserción
            insert_query = """
                INSERT INTO datalake_economico.anac (fecha, aeropuerto, cantidad) 
                VALUES (%s, %s, %s)
            """
            
            # Convertir DataFrame a lista de tuplas
            data_to_insert = []
            for _, row in df.iterrows():
                # Asegurar que la fecha sea date, no datetime
                fecha_date = row['fecha'].date() if hasattr(row['fecha'], 'date') else row['fecha']
                data_to_insert.append((fecha_date, row['aeropuerto'], float(row['cantidad'])))
            
            # Insertar en lotes para mejor performance
            batch_size = 500
            total_inserted = 0
            
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                try:
                    self.cursor.executemany(insert_query, batch)
                    self.conn.commit()
                    total_inserted += len(batch)
                    logger.info(f"Lote insertado: {len(batch)} registros (Total: {total_inserted})")
                except Exception as batch_error:
                    logger.error(f"Error en lote {i}: {batch_error}")
                    self.conn.rollback()
                    raise
            
            logger.info(f"[OK] Total insertados: {total_inserted} registros")

            # ---------------------------------------------------
            # VERIFICAR QUE LOS DATOS SE INSERTARON
            # ---------------------------------------------------
            verify_query = "SELECT COUNT(*) FROM datalake_economico.anac"
            self.cursor.execute(verify_query)
            total_after = self.cursor.fetchone()[0]
            logger.info(f"Total de registros en tabla después de inserción: {total_after}")
            
            # Verificar datos específicos de corrientes
            verify_corrientes_query = """
                SELECT COUNT(*) 
                FROM datalake_economico.anac 
                WHERE aeropuerto = 'corrientes'
            """
            self.cursor.execute(verify_corrientes_query)
            corrientes_count = self.cursor.fetchone()[0]
            logger.info(f"Registros de 'corrientes' en tabla: {corrientes_count}")
            
            if corrientes_count > 0:
                sample_corrientes_query = """
                    SELECT fecha, cantidad 
                    FROM datalake_economico.anac 
                    WHERE aeropuerto = 'corrientes' 
                    ORDER BY fecha DESC 
                    LIMIT 3
                """
                self.cursor.execute(sample_corrientes_query)
                sample = self.cursor.fetchall()
                logger.info(f"Muestra de datos de corrientes: {sample}")

            logger.info("=" * 50)
            logger.info(f"[OK] CARGA COMPLETADA PARA {ultima_fecha}")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Error al cargar datos a la base de datos: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def load_to_sheets(self, ultimo_valor, ultima_fecha):
        """
        Actualiza Google Sheets para la estructura específica:
        - Fila 3: Encabezados de meses (jul-25, ago-25, etc.)
        - Fila 10: "Pasajeros en el aeropuerto de Corrientes"
        - Se actualiza siempre en fila 10, en la columna del mes correspondiente
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

            # Convertir fecha a formato del Sheets (ej: "sept-25")
            from datetime import datetime, date
            
            if isinstance(ultima_fecha, (datetime, pd.Timestamp)):
                fecha_obj = ultima_fecha.date() if hasattr(ultima_fecha, 'date') else ultima_fecha
            elif isinstance(ultima_fecha, date):
                fecha_obj = ultima_fecha
            else:
                fecha_obj = pd.to_datetime(ultima_fecha).date()
            
            # Formato usado en el Sheets: "sept-25", "ago-25", etc.
            # Mapeo de meses en español (según tu imagen: "sept-25")
            mes_espanol = {
                1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
                7: "jul", 8: "ago", 9: "sept", 10: "oct", 11: "nov", 12: "dic"
            }
            
            fecha_formato_sheets = f"{mes_espanol[fecha_obj.month]}-{str(fecha_obj.year)[-2:]}"
            logger.info(f"Buscando columna para fecha: {fecha_formato_sheets}")
            
            # ------------------------------------------------------------
            # PASO 1: Leer la fila 3 (encabezados de meses)
            # ------------------------------------------------------------
            logger.info("Leyendo encabezados de meses (fila 3)...")
            headers_range = "Datos!3:3"  # Fila completa 3
            headers_result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=headers_range
            ).execute()
            
            headers = headers_result.get("values", [[]])[0]  # Primera (y única) fila
            logger.info(f"Encabezados en fila 3: {headers}")
            
            # Encontrar la columna correspondiente a la fecha
            columna_encontrada = None
            for i, header in enumerate(headers):
                if header and isinstance(header, str):
                    header_clean = header.strip().lower()
                    # Buscar coincidencia flexible
                    if (fecha_formato_sheets in header_clean or 
                        header_clean in fecha_formato_sheets or
                        header_clean.replace(" ", "") == fecha_formato_sheets):
                        columna_encontrada = i  # Índice base 0
                        logger.info(f"Fecha '{fecha_formato_sheets}' encontrada en columna índice {columna_encontrada}")
                        break
            
            if columna_encontrada is None:
                logger.error(f"No se encontró la columna para fecha '{fecha_formato_sheets}' en encabezados")
                logger.error("Encabezados disponibles: " + ", ".join([f"'{h}'" for h in headers if h]))
                
                # Intentar buscar el próximo mes disponible si el actual no existe
                logger.info("Intentando encontrar próxima columna disponible...")
                for i, header in enumerate(headers):
                    if header and isinstance(header, str):
                        # Buscar cualquier fecha futura
                        header_clean = header.strip().lower()
                        if any(mes in header_clean for mes in ["-24", "-25", "-26"]):
                            columna_encontrada = i
                            logger.info(f"Usando columna alternativa: '{header}' en índice {columna_encontrada}")
                            break
                
                if columna_encontrada is None:
                    # Si no encontramos, usar la última columna + 1
                    columna_encontrada = len([h for h in headers if h])
                    logger.info(f"Creando nueva columna en índice {columna_encontrada}")
            
            # ------------------------------------------------------------
            # PASO 2: Actualizar la celda en fila 10
            # ------------------------------------------------------------
            # Convertir índice de columna a letra (0->A, 1->B, etc.)
            letra_columna = self.num_to_col(columna_encontrada)
            rango_destino = f'Datos!{letra_columna}10'
            
            logger.info(f"Actualizando celda {rango_destino} (Fila 10, Columna {columna_encontrada}) con valor: {ultimo_valor}")
            
            # Leer valor actual para comparar
            try:
                valor_actual_result = sheet.values().get(
                    spreadsheetId=SPREADSHEET_ID,
                    range=rango_destino
                ).execute()
                
                valor_actual = valor_actual_result.get('values', [[]])[0][0] if valor_actual_result.get('values') else ""
                logger.info(f"Valor actual en {rango_destino}: '{valor_actual}'")
                
                # Verificar si ya tiene el mismo valor
                if valor_actual and str(valor_actual).strip() != "":
                    try:
                        valor_actual_float = float(str(valor_actual).replace(',', '.').strip())
                        if valor_actual_float == float(ultimo_valor):
                            logger.info(f"[SKIP] Celda {rango_destino} ya tiene el valor correcto: {valor_actual}")
                            return
                    except ValueError:
                        pass  # Si no se puede convertir, continuar con la actualización
            except Exception as e:
                logger.warning(f"No se pudo leer valor actual: {e}")
            
            # Actualizar la celda
            body = {'values': [[ultimo_valor]]}
            
            request = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=rango_destino,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            if request.get('updatedCells'):
                logger.info(f"[OK] Datos actualizados en Google Sheets: {request.get('updatedCells')} celdas en {rango_destino}")
            else:
                logger.warning("No se actualizaron celdas en Google Sheets.")
            
            # ------------------------------------------------------------
            # PASO 3: Verificar que se actualizó correctamente
            # ------------------------------------------------------------
            time.sleep(1)  # Esperar un momento
            
            # Leer la fila 6 completa para ver el contexto
            fila_10_range = 'Datos!10:10'
            fila_10_result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=fila_10_range
            ).execute()
            
            valores_fila_10 = fila_10_result.get('values', [[]])[0]
            logger.info(f"Verificación - Fila 10 completa: {valores_fila_10}")
            
            # También verificar las filas 3 y 6 juntas para contexto
            contexto_range = 'Datos!3:10'
            contexto_result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=contexto_range
            ).execute()
            
            logger.info("Contexto completo (filas 3 a 10):")
            fila_num = 3
            for fila in contexto_result.get('values', []):
                logger.info(f"  Fila {fila_num}: {fila}")
                fila_num += 1

        except Exception as e:
            logger.error(f"Error al actualizar Google Sheets: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def num_to_col(self, n):
        """
        Convierte un número de índice de columna a una letra de columna
        (0->A, 1->B, ..., 25->Z, 26->AA, 27->AB, etc.)
        """
        result = ""
        while n >= 0:
            result = chr(n % 26 + 65) + result
            n = n // 26 - 1
        return result

    def obtener_ultimo_valor_corrientes(self):
        """
        Obtiene el último valor de Corrientes desde la BD adaptado al nuevo esquema vertical.
        Retorna: (ultimo_valor_float, ultima_fecha) o (None, None)
        """
        try:
            # Asegurar conexión
            self.conectar_bdd()
            
            if not self.conn or not self.cursor:
                logger.error("No se pudo establecer conexión con la base de datos")
                return None, None

            # PRIMERO: Verificar si la tabla existe y tiene datos
            check_table_query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = 'anac'
            """
            self.cursor.execute(check_table_query, (self.database,))
            table_exists = self.cursor.fetchone()[0] > 0
            
            if not table_exists:
                logger.warning("La tabla 'anac' no existe")
                return None, None
            
            # Verificar total de registros
            count_query = "SELECT COUNT(*) FROM datalake_economico.anac"
            self.cursor.execute(count_query)
            total_registros = self.cursor.fetchone()[0]
            
            logger.info(f"Total de registros en tabla 'anac': {total_registros}")
            
            if total_registros == 0:
                logger.warning("La tabla 'anac' está vacía")
                return None, None
            
            # MOSTRAR estructura de tabla para debug
            describe_query = "DESCRIBE datalake_economico.anac"
            self.cursor.execute(describe_query)
            estructura = self.cursor.fetchall()
            logger.info(f"Estructura de tabla: {estructura}")
            
            # BUSCAR específicamente "corrientes"
            logger.info("Buscando datos de 'corrientes'...")
            
            # Opción 1: Buscar exacto
            select_query = """
                SELECT cantidad, fecha
                FROM datalake_economico.anac
                WHERE aeropuerto = 'corrientes'
                ORDER BY fecha DESC
                LIMIT 1
            """
            self.cursor.execute(select_query)
            result = self.cursor.fetchone()
            
            if result:
                ultimo_valor = self._convertir_a_float(result[0])
                ultima_fecha = result[1]
                logger.info(f"[ÉXITO] Encontrado: {ultimo_valor} para fecha {ultima_fecha}")
                return ultimo_valor, ultima_fecha
            
            # Opción 2: Buscar con LIKE
            logger.info("No encontrado exacto, buscando con LIKE...")
            select_like_query = """
                SELECT cantidad, fecha
                FROM datalake_economico.anac
                WHERE aeropuerto LIKE '%corrientes%'
                ORDER BY fecha DESC
                LIMIT 1
            """
            self.cursor.execute(select_like_query)
            result = self.cursor.fetchone()
            
            if result:
                ultimo_valor = self._convertir_a_float(result[0])
                ultima_fecha = result[1]
                logger.info(f"[ÉXITO] Encontrado con LIKE: {ultimo_valor} para fecha {ultima_fecha}")
                return ultimo_valor, ultima_fecha
            
            # Opción 3: Mostrar qué aeropuertos existen
            logger.info("Mostrando aeropuertos disponibles...")
            aeropuertos_query = """
                SELECT DISTINCT aeropuerto, COUNT(*) as count
                FROM datalake_economico.anac
                GROUP BY aeropuerto
                ORDER BY aeropuerto
                LIMIT 10
            """
            self.cursor.execute(aeropuertos_query)
            aeropuertos = self.cursor.fetchall()
            logger.info(f"Primeros 10 aeropuertos: {aeropuertos}")
            
            # Opción 4: Mostrar cualquier dato para debug
            any_data_query = """
                SELECT fecha, aeropuerto, cantidad
                FROM datalake_economico.anac
                ORDER BY fecha DESC
                LIMIT 5
            """
            self.cursor.execute(any_data_query)
            any_data = self.cursor.fetchall()
            logger.info(f"Últimos 5 registros: {any_data}")
            
            logger.warning("No se encontraron datos para 'corrientes'")
            return None, None
            
        except Exception as e:
            logger.error(f"Error al leer datos de la base de datos: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None

    def _obtener_ultima_fecha_bd(self):
        """Obtiene la fecha más reciente en la base de datos y la devuelve como datetime.date"""
        self.conectar_bdd()

        if not self.conn or not self.cursor:
            return None

        try:
            # Verificar si la tabla existe CON ESQUEMA COMPLETO
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

            # Obtener la última fecha CON ESQUEMA COMPLETO
            select_max_date_query = "SELECT MAX(fecha) FROM datalake_economico.anac"
            self.cursor.execute(select_max_date_query)
            result = self.cursor.fetchone()

            if result and result[0]:
                ultima = result[0]
                # Normalizar a datetime.date
                if isinstance(ultima, (datetime, pd.Timestamp)):
                    ultima_date = ultima.date()
                elif isinstance(ultima, date):
                    ultima_date = ultima
                else:
                    # Convertir string a date
                    ultima_date = pd.to_datetime(ultima).date()

                logger.info(f"Última fecha en BD: {ultima_date}")
                return ultima_date
            else:
                logger.info("No hay datos en la tabla")
                return None

        except Exception as e:
            logger.error(f"Error al consultar última fecha: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    @staticmethod
    def _convertir_a_float(value):
        """Convierte un valor a float"""
        try:
            if value is None:
                return None
            value_str = str(value).strip().replace(',', '.')
            return float(value_str)
        except (ValueError, TypeError) as e:
            logger.warning(f"No se pudo convertir a float: {value} - Error: {e}")
            return None

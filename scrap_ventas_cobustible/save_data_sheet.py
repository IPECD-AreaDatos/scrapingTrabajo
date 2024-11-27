from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from pymysql import connect
from dotenv import load_dotenv
from json import loads
import logging

class ReadSheets:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

        # Configuración de logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def conectar_bdd(self):
        try:
            self.conn = connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Conexión a la base de datos establecida con éxito.")
            return self
        except Exception as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            raise

    def cargar_datos(self):
        #Link del documento: https://docs.google.com/spreadsheets/d/1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI/edit?gid=0#gid=0
        try:
            # Cargar variables de entorno
            load_dotenv()

            # Consulta SQL para obtener los datos de la base de datos
            query_select = 'SELECT fecha, cantidad FROM combustible WHERE provincia = 18 and fecha >= "2018-12-01"'
            df = pd.read_sql(query_select, self.conn)

            # Agrupar por año y mes, sumando las cantidades
            df_combustible = df.groupby(df['fecha'].dt.to_period('M'))['cantidad'].sum()

            # Resetear el índice y convertir a lista
            df_combustible = df_combustible.reset_index(drop=True)
            lista_combustible = df_combustible.tolist()

            self.logger.info(f"Datos extraídos y procesados. Total de registros: {len(lista_combustible)}")

            # Configuración de Google Sheets API
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

            # Cargar la clave de la API desde el entorno
            key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

            # ID del documento de Google Sheets
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

            # Cargar las credenciales
            creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

            # Crear una instancia del servicio Sheets
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()

            # Realizar la actualización en el Google Sheet
            request = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range='Datos!C6:6',
                valueInputOption='RAW',
                body={'values': [lista_combustible]}
            ).execute()

            # Verificar si la solicitud fue exitosa
            if request.get('updatedCells'):
                self.logger.info(f"Datos actualizados en Google Sheets: {request.get('updatedCells')} celdas.")
            else:
                self.logger.warning("No se actualizaron celdas en Google Sheets.")
        except Exception as e:
            self.logger.error(f"Error al cargar los datos: {e}")
            raise
        finally:
            # Cerrar la conexión a la base de datos
            self.cerrar_conexion()

    def cerrar_conexion(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.logger.info("Conexión a la base de datos cerrada.")
        except Exception as e:
            self.logger.error(f"Error al cerrar la conexión a la base de datos: {e}")


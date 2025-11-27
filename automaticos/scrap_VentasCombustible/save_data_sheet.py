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

    def cargar_datos(self, suma_fecha):
        try:
            # Cargar variables de entorno
            load_dotenv()

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

            # Leer los valores de la fila 6 para obtener la última celda ocupada
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range='Datos!6:6'  # Leer toda la fila 6
            ).execute()

            values = result.get('values', [])

            # Si la fila 6 está vacía o no tiene datos
            if not values or not values[0]:
                # Si la fila está vacía, colocar en la primera celda (A6)
                last_column = 0
            else:
                # Encontrar la última celda no vacía en la fila 6
                last_column = len(values[0])

            # Convertir el índice de la columna en letra (A, B, C, ..., Z, AA, AB, ...)
            column_letter = self.num_to_col(last_column)  # Usamos nuestra función personalizada

            # El rango adecuado sería algo como Datos!A6, Datos!B6, ..., Datos!BU6, etc.
            range_to_update = f'Datos!{column_letter}6'

            # Actualizar la celda con el valor de la suma
            request = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_to_update,  # Cambiar 'Datos!6' por la celda específica
                valueInputOption='RAW',
                body={'values': [[suma_fecha]]}  # Aquí colocamos el valor de la suma como un array
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

    def num_to_col(self, n):
        """Convierte un número de índice de columna a una letra de columna (A, B, C, ..., Z, AA, AB, ...)"""
        result = ""
        while n >= 0:
            result = chr(n % 26 + 65) + result
            n = n // 26 - 1
        return result

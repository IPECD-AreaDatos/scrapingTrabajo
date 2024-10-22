from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from pymysql import connect
from dotenv import load_dotenv
from json import loads




class readSheets:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    

    def cargar_datos(self):

        #Cargamos varibles de entorno
        load_dotenv()

        query_select = 'SELECT fecha, cantidad FROM combustible WHERE provincia = 18 and fecha >= "2018-12-01"'
        df = pd.read_sql(query_select, self.conn)

        # Agrupar por año y mes y sumar las cantidades
        df_combustible = df.groupby(df['fecha'].dt.to_period('M'))['cantidad'].sum()

        # Eliminar la columna de fecha si no es necesaria
        df_combustible = df_combustible.reset_index(drop=True)

        # Convierte el resultado a una lista de Python
        lista_combustible = df_combustible.tolist()

        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        #Remplzamos los datos en la fila correspondiente
        request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C6:6',
                                        valueInputOption='RAW',
                                        body={'values':[lista_combustible]}).execute()


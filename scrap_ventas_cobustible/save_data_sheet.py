from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
import pymysql
import mysql
import mysql.connector


class readSheets:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    

    def cargar_datos(self):
        query_select = 'SELECT fecha, cantidad FROM combustible WHERE provincia = 18 and fecha >= "2018-12-01"'
        df = pd.read_sql(query_select, self.conn)

        # Agrupar por año y mes y sumar las cantidades
        df_combustible = df.groupby(df['fecha'].dt.to_period('M'))['cantidad'].sum()

        # Eliminar la columna de fecha si no es necesaria
        df_combustible = df_combustible.reset_index(drop=True)

        # Convierte el resultado a una lista de Python
        lista_combustible = df_combustible.tolist()

        print(lista_combustible)

        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        #Direccion del archivo json 
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        KEY = os.path.join(directorio_desagregado, 'key.json')

        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()


        #Remplzamos los datos en la fila correspondiente
        request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C6:6',
                                        valueInputOption='RAW',
                                        body={'values':[lista_combustible]}).execute()

""""
#Zona de pruebas

host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'
database = 'datalake_economico'

conn = pymysql.connect(host = host,user=user,password= password, database=database)

query_select = 'SELECT * FROM combustible WHERE provincia = 18 and fecha >= "2018-12-01"'
df = pd.read_sql(query_select,conn)

readSheets().cargar_datos(df)


"""
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
from json import loads
from pymysql import connect
from pandas import read_sql,to_datetime

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

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

                #Buscamos los datos de la bdd
                df = read_sql('SELECT * FROM dnrpa',self.conn)

                df['fecha'] = to_datetime(df['fecha'])

                #Creacion de listas
                autos = df['cantidad'][(df['id_provincia_indec'] == 18) & (df['id_vehiculo'] == 1) & (df['fecha'] >= '2018-12-01')]
                
                # Eliminar todos los ceros
                autos = [dato for dato in autos if dato != 0]
                lista_de_lista_autos = [autos]

                motos = df['cantidad'][(df['id_provincia_indec'] == 18)  & (df['id_vehiculo'] == 2)  & (df['fecha'] >= '2018-12-01')]

                motos = [dato for dato in motos if dato != 0]
                lista_de_lista_motos = [motos]

                # Define los alcances y la ruta al archivo JSON de credenciales
                SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
                
                #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
                key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

                #ID del documento:
                SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
                
                #Documento ------> https://docs.google.com/spreadsheets/d/1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI/edit?gid=0#gid=0

                # Carga las credenciales desde el archivo JSON
                creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

                # Crea una instancia de la API de Google Sheets
                service = build('sheets', 'v4', credentials=creds)
                sheet = service.spreadsheets()

                #Remplzamos los datos en la fila correspondiente
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Datos!C7:7',valueInputOption='RAW',body={'values':lista_de_lista_autos}).execute()
                
                #Remplzamos los datos en la fila correspondiente
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Datos!C8:8',valueInputOption='RAW',body={'values':lista_de_lista_motos}).execute()
                

        def main(self):               
                self.conectar_bdd()
                self.cargar_datos()

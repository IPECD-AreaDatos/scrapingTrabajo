from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
from json import loads
from pymysql import connect
import pandas as pd
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
        
        def lista_vehiculo(self, provincia):
                df = read_sql('SELECT * from dnrpa', self.conn)

                df['fecha'] = to_datetime(df['fecha'])

                #Creacion de listas
                autos = df['cantidad'][(df['id_provincia_indec'] == provincia) & (df['id_vehiculo'] == 1) & (df['fecha'] >= '2018-12-01')]
                
                # Eliminar todos los ceros
                autos = [dato for dato in autos if dato != 0]
                lista_de_lista_autos = [autos]

                motos = df['cantidad'][(df['id_provincia_indec'] == provincia)  & (df['id_vehiculo'] == 2)  & (df['fecha'] >= '2018-12-01')]

                motos = [dato for dato in motos if dato != 0]
                lista_de_lista_motos = [motos]

                return lista_de_lista_autos, lista_de_lista_motos
        
        def lista_vehiculo_nacion(self):
                df = read_sql('SELECT * from dnrpa', self.conn)

                df['fecha'] = to_datetime(df['fecha'])

                #Creacion de listas
                #autos = df['cantidad'][(df['id_vehiculo'] == 1) & (df['fecha'] >= '2018-12-01')]
                autos = (df[(df['id_vehiculo'] == 1) & (df['fecha'] >= '2018-12-01')].groupby(pd.Grouper(key='fecha', freq='MS'))['cantidad'].sum())

                # Eliminar todos los ceros
                autos = [dato for dato in autos if dato != 0]
                lista_de_lista_autos = [autos]

                #motos = df['cantidad'][(df['id_vehiculo'] == 2)  & (df['fecha'] >= '2018-12-01')]
                motos = (df[(df['id_vehiculo'] == 2) & (df['fecha'] >= '2018-12-01')].groupby(pd.Grouper(key='fecha', freq='MS'))['cantidad'].sum())

                motos = [dato for dato in motos if dato != 0]
                lista_de_lista_motos = [motos]

                return lista_de_lista_autos, lista_de_lista_motos

        def cargar_datos_ctes(self, lista_de_lista_autos, lista_de_lista_motos):

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
                
        def cargar_datos_nea(self, lista_de_lista_autos_ch, lista_de_lista_motos_ch, lista_de_lista_autos_ms, lista_de_lista_motos_ms, lista_de_lista_autos_fs, lista_de_lista_motos_fs, lista_de_lista_autos_nc, lista_de_lista_motos_nc):

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
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Autos!B2:2',valueInputOption='RAW',body={'values':lista_de_lista_autos_ch}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Autos!B3:3',valueInputOption='RAW',body={'values':lista_de_lista_autos_ms}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Autos!B4:4',valueInputOption='RAW',body={'values':lista_de_lista_autos_fs}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Autos!B5:5',valueInputOption='RAW',body={'values':lista_de_lista_autos_nc}).execute()

                #Remplzamos los datos en la fila correspondiente
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Motos!B2:2',valueInputOption='RAW',body={'values':lista_de_lista_motos_ch}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Motos!B3:3',valueInputOption='RAW',body={'values':lista_de_lista_motos_ms}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Motos!B4:4',valueInputOption='RAW',body={'values':lista_de_lista_motos_fs}).execute()
                request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,range='Patentamiento Motos!B5:5',valueInputOption='RAW',body={'values':lista_de_lista_motos_nc}).execute()

        def main(self):               
                self.conectar_bdd()
                lista_de_lista_autos_ctes, lista_de_lista_motos_ctes = self.lista_vehiculo(18)
                self.cargar_datos_ctes(lista_de_lista_autos_ctes, lista_de_lista_motos_ctes)
                lista_de_lista_autos_ch, lista_de_lista_motos_ch = self.lista_vehiculo(22)
                lista_de_lista_autos_ms, lista_de_lista_motos_ms = self.lista_vehiculo(54)
                lista_de_lista_autos_fs, lista_de_lista_motos_fs = self.lista_vehiculo(34)
                lista_de_lista_autos_nc, lista_de_lista_motos_nc = self.lista_vehiculo_nacion()
                self.cargar_datos_nea( lista_de_lista_autos_ch, lista_de_lista_motos_ch, lista_de_lista_autos_ms, lista_de_lista_motos_ms, lista_de_lista_autos_fs, lista_de_lista_motos_fs, lista_de_lista_autos_nc, lista_de_lista_motos_nc)


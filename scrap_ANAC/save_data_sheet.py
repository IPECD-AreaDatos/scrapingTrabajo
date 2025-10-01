from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from json import loads
from google.oauth2 import service_account
import os
import pandas as pd
from datetime import datetime


class readSheets:
    def escribir_fila(self, values, fecha_dato):
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
        
        # Buscar la última columna con datos en la fila 10 (Pasajeros en el aeropuerto de Corrientes)
        fila_pasajeros = 10
        
        # Leer un rango más amplio para encontrar todas las columnas con datos (hasta CZ)
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'Datos!A{fila_pasajeros}:CZ{fila_pasajeros}'
        ).execute()
        
        row_values = result.get('values', [[]])[0] if result.get('values') else []
        
        # Encontrar la última columna que realmente tiene datos (no vacía)
        ultima_columna_con_datos = 0
        for i, valor in enumerate(row_values):
            if valor and str(valor).strip():  # Si el valor no está vacío
                ultima_columna_con_datos = i + 1
        
        print(f"Última columna con datos encontrada: {ultima_columna_con_datos}")
        
        # Leer las cabeceras de fecha (fila 3) para verificar el período más reciente
        result_headers = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'Datos!A3:CZ3'
        ).execute()
        
        header_values = result_headers.get('values', [[]])[0] if result_headers.get('values') else []
        
        # Convertir número de columna a letra (A=1, B=2, etc.)
        def numero_a_columna(num):
            resultado = ""
            while num > 0:
                num -= 1
                resultado = chr(65 + num % 26) + resultado
                num //= 26
            return resultado
        
        # Verificar si ya existe una columna para el período del dato
        if isinstance(fecha_dato, str):
            fecha_obj = datetime.strptime(fecha_dato, "%Y-%m-%d")
        else:
            fecha_obj = fecha_dato
        periodo_dato = fecha_obj.strftime("%b-%y").lower()  # ej: "ago-25"
        
        periodo_encontrado = False
        for i, header in enumerate(header_values):
            if header and periodo_dato in str(header).lower():
                periodo_encontrado = True
                proxima_columna = i + 1
                print(f"Período {periodo_dato} ya existe en columna {numero_a_columna(proxima_columna)}")
                break
        
        if not periodo_encontrado:
            # La próxima columna disponible
            proxima_columna = ultima_columna_con_datos + 1
            print(f"Creando nueva columna para período {periodo_dato}")
        
        letra_columna = numero_a_columna(proxima_columna)
        rango_destino = f'Datos!{letra_columna}{fila_pasajeros}'
        
        print(f"Escribiendo en: {rango_destino} (última columna con datos: {numero_a_columna(ultima_columna_con_datos)})")
        
        # Asegúrate de que `values` sea un único valor para una celda
        body = {'values': [[values]]}
        
        # Actualizar la hoja de cálculo en la próxima columna disponible
        request = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_destino,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"{request.get('updatedCells')} celda(s) actualizada(s) en {rango_destino}!")
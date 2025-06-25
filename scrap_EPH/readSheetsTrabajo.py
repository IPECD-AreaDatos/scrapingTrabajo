from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from json import loads
import json

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv

"""
Este script saca los datos de la hoja de sheets: https://docs.google.com/spreadsheets/d/1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM/edit?gid=0#gid=0
De la hoja EPH
"""

class readSheetsTrabajoEPH:
    def leer_datos_tasas(self):
        load_dotenv()
        # Setup
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'
        RANGE = 'Trabajo_EPH!A1:I1000'  # Ampliamos el rango por seguridad

        # API call
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
        values = result.get('values', [])

        print(f"🔎 Filas obtenidas crudas (incluyendo encabezado): {len(values)}")

        if len(values) <= 1:
            print("⚠️ No se encontraron datos suficientes (solo encabezado o vacío).")
            return pd.DataFrame()

        # Crear DataFrame
        df = pd.DataFrame(values[1:], columns=[
            'Region', 'Aglomerado', 'Año', 'Fecha', 'Trimestre',
            'Estado del dato', 'Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación'
        ])

        # Limpieza inicial
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Limpia espacios
        df.replace({"": pd.NA, " ": pd.NA}, inplace=True)

        # Eliminar filas no finalizadas
        df = df[df['Estado del dato'] == 'FINALIZADO']
        df.drop(['Estado del dato'], axis=1, inplace=True)

        # Convertir tipos
        self.transformar_tipo_datos(df)

        # Prints de control
        print(f"✅ Filas luego de limpieza y filtro 'FINALIZADO': {len(df)}")
        print("📌 Columnas del DataFrame:", df.columns.tolist())
        print("📄 Tipos de datos:\n", df.dtypes)
        print("🧾 Últimas filas:\n", df.tail())

        return df

    def transformar_tipo_datos(self, df):
        df['Año'] = pd.to_numeric(df['Año'], errors='coerce')
        df['Tasa de Actividad'] = pd.to_numeric(df['Tasa de Actividad'], errors='coerce')
        df['Tasa de Empleo'] = pd.to_numeric(df['Tasa de Empleo'], errors='coerce')
        df['Tasa de desocupación'] = pd.to_numeric(df['Tasa de desocupación'], errors='coerce')
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación']
        # Convertir las columnas numéricas a tipos numéricos
        # Eliminar el símbolo "%" y las comas, y luego convertir las columnas en valores numéricos
        df[columnas_numericas] = df[columnas_numericas].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)

        # Dividir los valores numéricos por 100
        df[columnas_numericas] = df[columnas_numericas] / 100
        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)
        df['Region'] = df['Region'].astype(str)
        df['Año'] = df['Año'].astype(int)
        
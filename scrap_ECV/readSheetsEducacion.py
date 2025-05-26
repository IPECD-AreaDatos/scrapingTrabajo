from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from json import loads


class readSheetsEducacion:
    def leer_datos_educacion(self):
        # Define los alcances y carga variables de entorno
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        load_dotenv()

        # Cargamos la key y las credenciales
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # ID del Spreadsheet
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'

        # Construimos el servicio de Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Obtenemos los datos
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Educacion_ECV!A:AA').execute()
        values = result.get('values', [])

        if not values or len(values) < 2:
            print("No se encontraron datos válidos.")
            return pd.DataFrame()

        # El primer registro son los encabezados
        headers = ['aglomerado', 'año', 'trimestre', 'fecha', 'nivel_educativo', 'estado_del_dato',
                   'asiste', 'no_asiste_pero_asistio', 'nunca_asistio',
                   'institucion_publica', 'institucion_privada', 'edad_promedio_abandono',
                   'sobreedad', 'acceso_a_internet_fijo', 'calidad_de_vivienda_suficiente',
                   'calidad_de_vivienda_parcialmente_insuficiente', 'calidad_de_vivienda_insuficiente',
                   'vivienda_cercana_a_un_basural', 'vivienda_en_villa_emergencia',
                   'automovil', 'motocicleta', 'bicicleta', 'caminata', 'taxi_o_remis',
                   'transporte_urbano', 'otros', 'estudia_desde_casa']

        df = pd.DataFrame(values[1:], columns=headers)

        # Filtrar solo registros FINALIZADOS
        df = df[df['estado_del_dato'] == 'FINALIZADO']

        # Limpiar y normalizar datos
        df.replace({"": np.nan, " ": np.nan}, inplace=True)

        # Eliminar columna de estado
        df.drop(columns=['estado_del_dato'], inplace=True)

        # Normalizar tipos de datos
        self.transformar_tipo_datos(df)

        print(df)
        return df

    def transformar_tipo_datos(self, df):
        # Definir columnas numéricas
        columnas_numericas = ['asiste', 'no_asiste_pero_asistio', 'nunca_asistio',
                              'institucion_publica', 'institucion_privada', 'sobreedad',
                              'acceso_a_internet_fijo', 'calidad_de_vivienda_suficiente',
                              'calidad_de_vivienda_parcialmente_insuficiente',
                              'calidad_de_vivienda_insuficiente', 'vivienda_cercana_a_un_basural',
                              'vivienda_en_villa_emergencia', 'automovil', 'motocicleta',
                              'bicicleta', 'caminata', 'taxi_o_remis', 'transporte_urbano',
                              'otros', 'estudia_desde_casa']

        for col in columnas_numericas:
            if col in df.columns:
                df[col] = df[col].replace({'%': '', ',': '.'}, regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col] / 100  # Asumiendo que son porcentajes

        # Fecha
        df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y', errors='coerce')

        # Convertir a tipos apropiados
        df['trimestre'] = df['trimestre'].astype(str)
        df['nivel_educativo'] = df['nivel_educativo'].astype(str)
        df['aglomerado'] = df['aglomerado'].astype(str)
        df['año'] = pd.to_numeric(df['año'], errors='coerce').astype('Int64')

        # Edad promedio de abandono
        df['edad_promedio_abandono'] = pd.to_numeric(df['edad_promedio_abandono'], errors='coerce')




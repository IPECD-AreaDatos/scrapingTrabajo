from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd



class readSheetsSaludQuintilCoberturaEst:
    def leer_datos_salud_quintil_cobertura_est(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

               # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        #Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Salud_Q_Cob_Est!A:J').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]
        
        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['Aglomerado', 'Año', 'Fecha', 'Semestre', 'Quintil', 'Obra social/Prepaga', 'Planes y seguros', 'Sin Cobertura', 'Establecimiento_Privado', 'Establecimiento_Publico'])
        print(df)
        print(df.dtypes)
        self.transformar_tipo_datos(df)

        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['Obra social/Prepaga', 'Planes y seguros', 'Sin Cobertura', 'Establecimiento_Privado', 'Establecimiento_Publico']
        # Convertir las columnas numéricas a tipos numéricos
        # Elimina el símbolo "%" y las comas, y luego convierte las columnas en valores numéricos
        for columna in columnas_numericas:
            # Crea una serie booleana que indica si la celda no es nula
            no_nulos = df[columna].notnull()
            # Aplica la conversión solo a las celdas no nulas
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna] / 100
        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        # Convertir la segunda columna a tipo de datos entero
        df['Semestre'] = df['Semestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)
        df['Año']= df['Año'].astype(int)
        df['Quintil']= df['Quintil'].astype(int)

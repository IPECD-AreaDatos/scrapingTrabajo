from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd

class readSheetsCensoIPECD:
    def leer_datos_censo(self):
        df=[]

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        directorio_actual= os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_guardado = os.path.join(directorio_actual, 'files')
        archivo_KEY = os.path.join(ruta_carpeta_guardado, 'key.json')

        spreadsheets_ID= '1IBOsYSVDWs9Tz1BN0OlGOZpTsoRNwmiurFJFg__L5ao'

        creds = service_account.Credentials.from_service_account_file(archivo_KEY, scopes=SCOPES)

        service= build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()

        result = sheet.values().get(spreadsheetId=spreadsheets_ID, range='Datos para mapa subir').execute()

        values = result.get('values', [])[2:]

        df = pd.DataFrame(values, columns=['Id_Departamento','Departamento','Poblacion 2010', 'Poblacion 2022', 'Variacion relativa %', 'Densidad de habitantes por KM2', 'Poblacion 2022 mujer excluye situacion de calle', 'Poblacion 2022 varon excluye situacion de calle', 'Indice de feminidad', '2022 Índice de envejecimiento (total de personas de 65 años o más/total de personas de 0 a 14 años de edad)*100','2010 Índice de envejecimiento (total de personas de 65 años o más / total de personas de 0 a 14 años de edad)*100', '2022 índice de dependencia potencial ( total de personas de 0 a 14 + total de personas de 65 o más)/personas de 15 a 64)', '2010 índice de dependencia potencial ( total de personas de 0 a 14 + total de personas de 65 o más)/personas de 15 a 64)', 'Tasa de empleo', 'Tasa de desocup', 'Tasa de actividad', 'Cateogría ocupacional: Servicio doméstico', 'Categoría Ocupacional: Empleada(o) u obrera(o)', 'Categoría Ocupacional: Cuenta propia', 'Categoría Ocupacional: Patrón(a) o empleador(a)', 'Categoría Ocupacional: Trabajador(a) familiar', 'Categoría Ocupacional: Ignorado', 'Población que asiste a institución educativa', 'Población que no asiste pero asistió a inst educativa', 'Población que nunca asistió a inst educativa', 'Población en viviendas particulares que asiste a escuelas: nivel educativo Jardin maternal, guardería, centro de cuidado, salas de 0 a 3', 'Población en viviendas particulares que asiste a escuelas: nivel educativo sala de 4 o 5 años', 'Población en viviendas particulares que asiste a escuelas: nivel educativo primario', 'Población en viviendas particulares que asiste a escuelas: nivel educativo secundario', 'Población en viviendas particulares que asiste a escuelas: nivel educativo terciario no universitario', 'Población en viviendas particulares que asiste a escuelas: nivel educativo universitario de grado', 'Población en viviendas particulares que asiste a escuelas: nivel educativo posgrado', 'Mujeres de 14 a 49 años con al menos 1 hijo nacido vivo', 'Promedio de hijos por mujer', 'Población en vivienda: Obra Social o prepaga (incluye PAMI)', 'Población en vivienda: Programas o planes estatales de salud', 'Población en vivienda: No tiene obra social, prepaga ni plan estatal'])
        print(df)
        print(df.dtypes)
        print(df.columns)
        return df


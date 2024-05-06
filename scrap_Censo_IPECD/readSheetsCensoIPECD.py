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

        df = pd.DataFrame(
            values,
            columns=[
                'Id_Departamento',
                'Departamento',
                'Poblacion_2010',
                'Poblacion_2022',
                'Variacion_relativa',
                'Densidad_de_habitantes_por_KM2',
                'Poblacion_2022_mujer_excluye_situacion_de_calle',
                'Poblacion_2022_varon_excluye_situacion_de_calle',
                'Indice_de_feminidad',
                'Envejecimiento_2022',
                'Envejecimiento_2010',
                'Dependencia_potencial_2022',
                'Dependencia_potencial_2010',
                'Tasa_de_empleo',
                'Tasa_de_desocup',
                'Tasa_de_actividad',
                'Cuenta_propia',
                'Empleada_o_obrera',
                'Patrón_o_empleador',
                'Trabajador_familiar',
                'Ignorado',
                'Asiste_a_institucion_educativa',
                'No_asiste_pero_asistió_inst_educativa',
                'Nunca_asistió_inst_educativa',
                'Asiste_a_escuelas_jardin_maternal',
                'Asiste_a_escuelas_sala_de_4_o_5_años',
                'Asiste_a_escuelas_primario',
                'Asiste_a_escuelas_secundario',
                'Asiste_a_escuelas_terciario_no_universitario',
                'Asiste_a_escuelas_universitario_de_grado',
                'Asiste_a_escuelas_posgrado',
                'Mujeres_con_al_menos_1_hijo',
                'Promedio_de_hijos_por_mujer',
                'Obra_social_o_prepaga',
                'Programas_o_planes_estatales_de_salud',
                'No_tiene_obra_social_prepaga_ni_plan_estatal'
            ]
        )
        print(df)
        print(df.dtypes)
        print(df.columns)
        return df


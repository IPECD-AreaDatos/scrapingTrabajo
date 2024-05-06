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

        result = sheet.values().get(spreadsheetId=spreadsheets_ID, range='Equipo Datos!A:AK').execute()

        values = result.get('values', [])[1:]

        df = pd.DataFrame(
            values,
            columns=[
                'id_departamento',
                'departamento',
                'poblacion_2010',
                'poblacion_2022',
                'variacion_relativa',
                'densidad_de_habitantes_por_KM2',
                'poblacion_2022_mujer_excluye_situacion_de_calle',
                'poblacion_2022_varon_excluye_situacion_de_calle',
                'indice_de_feminidad',
                '_2022_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100',
                '_2010_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100',
                '_2022_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64',
                '_2010_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64',
                'tasa_de_empleo',
                'tasa_de_desocup',
                'tasa_de_actividad',
                'categoria_ocupacional_servicio_domestico',
                'categoria_ocupacional_empleado_u_obrero',
                'categoria_ocupacional_cuenta_propia',
                'categoria_ocupacional_patron_o_empleador',
                'categoria_ocupacional_trabajador_familiar',
                'categoria_ocupacional_ignorado',
                'población_que_asiste_a_institución_educativa',
                'población_que_no_asiste_pero_asistio_a_institución_educativa',
                'población_que_nunca_asistio_a_institución_educativa',
                'pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03',
                'pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5',
                'pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario',
                'pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario',
                'pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers',
                'pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado',
                'pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado',
                'mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo',
                'promedio_de_hijos_por_mujer',
                'población_en_vivienda_obra_social_o_prepaga_incluye_PAMI',
                'población_en_vivienda_programas_o_planes_estatales_de_salud',
                'población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal'
            ]
            )
        df = df.dropna()
        # Convertir las columnas específicas a los tipos de datos deseados
        # Convertir todas las columnas a tipo float
        for col in df.columns:
            # Eliminar caracteres no numéricos y convertir a tipo flotante
            df[col] = df[col].str.replace(r'[^\d.]', '', regex=True)
            df[col] = df[col].str.replace(',', '.')
            df[col] = df[col].str.replace('%', '')
            df[col] = df[col].astype(float)
        df['id_departamento'] = df['id_departamento'].astype(int)
        df['departamento'] = df['departamento'].astype(str)
        df['poblacion_2010'] = df['poblacion_2010'].astype(int)
        df['poblacion_2022'] = df['poblacion_2022'].astype(int)
        # Convertir todas las demás columnas a tipo float

        df = df.astype(float)

        print(df)
        print(df.dtypes)
        print(df.columns)
        return df


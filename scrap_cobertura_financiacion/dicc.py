import pandas as pd
import os

class Diccionario:
    def construir_dicc(self):
        
        #Creamos direcciones para acceder al archivo
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')

        # Construir las rutas de los archivos
        file_path = os.path.join(ruta_carpeta_files, 'dicc.xlsx')

        #Leemos archivos
        df = pd.read_excel(file_path, sheet_name=0, skiprows=0)
        df = df.drop(index=0).reset_index(drop=True)
        # cambiamos a minusculas todas las columnas
        df.columns = df.columns.str.lower()

        # Renombramos columnas para mayor claridad
        df.rename(columns={
            'descripción sección': 'desc_seccion',
            'descripción grupo': 'desc_grupo',
            'descripción ciiu': 'desc_ciiu',
            'sección': 'seccion'
        }, inplace=True)

        # convertimos a string algunas columnas
        columnas_str = ['seccion', 'desc_seccion', 'desc_grupo', 'ciiu', 'desc_ciiu', 'concatenado']
        df.loc[:, columnas_str] = df[columnas_str].astype(str)

        # Creamos los tres diccionarios separados eliminando duplicados
        df_seccion = df[['seccion', 'desc_seccion']].drop_duplicates()
        df_grupo = df[['grupo', 'desc_grupo']].drop_duplicates()
        df_ciiu = df[['ciiu', 'desc_ciiu']].drop_duplicates()

        # Imprimir los dataframes para verificar
        print("🔹 Diccionario Sección:")
        print(df_seccion)
        print("\n🔹 Diccionario Grupo:")
        print(df_grupo)
        print("\n🔹 Diccionario CIIU:")
        print(df_ciiu)

        return df_seccion, df_grupo, df_ciiu 


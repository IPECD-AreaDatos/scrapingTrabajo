import os
import pandas as pd
import datetime

df = pd.DataFrame() 

class readFile:
    def __init__(self):
        pass

    def read_file(self):
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Empresas en actividad.xls')
    

        df = pd.read_excel(file_path_desagregado, skiprows=4)
        df = df.dropna()
        df = df.rename(columns={'Unnamed: 0':'fecha', 'Corrientes ':'cantidad_empresas_corrientes', 'Total País':'cantidad_empresas_total_pais', 'Corrientes':'var_interanual_corrientes', 'Total País.1':'var_interanual_total_pais' })
        df['fecha'] = pd.to_datetime(df['fecha'])
        colums =['cantidad_empresas_corrientes', 'cantidad_empresas_total_pais', 'var_interanual_corrientes', 'var_interanual_total_pais']
        df['var_interanual_corrientes'] = df['var_interanual_corrientes'].replace({'...': 0}, regex=False)
        df['var_interanual_total_pais'] = df['var_interanual_total_pais'].replace({'...': 0}, regex=False)
        df['cantidad_empresas_corrientes'] = pd.to_numeric(df['cantidad_empresas_corrientes'])
        for colum in colums:
            df[colum] = pd.to_numeric(df[colum])
        print(df)
import os
import pandas as pd
import datetime

df = pd.DataFrame() 
class readFile2:
    def __init__(self):
        pass
    def create_df(self):
        
        #lee el archivo
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'Puestos de trabajo registrados.xls')
    
        #crea el df
        df = pd.read_excel(file_path_desagregado, skiprows=5)

        #renombra columnas
        df = df.rename(columns={'Unnamed: 0':'fecha', 'Unnamed: 1':'Corrientes', 'Unnamed: 2':'Pais', 'Mensual':'Corrientes_variacion_mensual', 'Interanual':'Corrientes_variacion_interanual', 'Acumulada':'Corrientes_variacion_acumulada', 'Mensual.1':'Pais_variacion_mensual', 'Interanual.1':'Pais_variacion_interanual', 'Acumulada.1':'Pais_variacion_acumulada'})
        
        #borra 3 ultimas filas
        df = df.iloc[:-3]


        numeric_columns =['Corrientes_variacion_mensual','Corrientes_variacion_interanual', 'Corrientes_variacion_acumulada','Pais_variacion_mensual', 'Pais_variacion_interanual','Pais_variacion_acumulada']
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        for colum in numeric_columns:
            df[colum] = df[colum].replace({'...':0}, regex=False)


        for colum in numeric_columns:
            df[colum] = pd.to_numeric(df[colum])

        print(df)
        print(df.columns)
        print(df.dtypes)
        return df

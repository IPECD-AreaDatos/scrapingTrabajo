import os
import pandas as pd
import numpy as np

def datos_sp():
    
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_name = "salarioPromedioSP.csv"
    file_path = os.path.join(ruta_carpeta_files, file_name)
            
    # Leer el archivo de csv y hacer transformaciones
    df = pd.read_csv(file_path) 
    df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

    #Sustitucion de vlaores negativos 
    df.loc[df['w_mean'] < 0,'w_mean'] = 0

    #Cambio de nombre de columna
    df = df.rename(columns={'w_mean':'salario'})

    return df


def datos_totales():

    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_name = "salarioPromedioTotal.csv"
    file_path = os.path.join(ruta_carpeta_files, file_name)
            
    # Leer el archivo de csv y hacer transformaciones
    df = pd.read_csv(file_path) 
    df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

        #Sustitucion de vlaores negativos 
    df.loc[df['w_mean'] < 0,'w_mean'] = 0

    #Cambio de nombre de columna
    df = df.rename(columns={'w_mean':'salario'})

    return df


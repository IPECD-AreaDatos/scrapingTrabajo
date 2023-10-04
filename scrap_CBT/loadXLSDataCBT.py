import datetime
import time
import os
from openpyxl import load_workbook
import pandas as pd

class loadXLSDataCBT:
    def readData(self):
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_destino = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')
        
        # Crear un DataFrame con las columnas de encabezado
        columnas_encabezado = [
            'Fecha',
            'CBA_Adulto',
            'CBT_Adulto',
            'CBA_Hogar',
            'CBT_Hogar',
            'CBA_NEA_Adulto',
            'CBT_NEA_Adulto',
            'CBA_NEA_Hogar',
            'CBT_NEA_Hogar'
        ]
        # Crear un DataFrame con una fila de encabezados
        df_encabezado = pd.DataFrame(columns=columnas_encabezado)
        print(df_encabezado)
        
        # Leer el archivo Excel y seleccionar las columnas 1, 2 y 4 desde la fila 8
        df_primeraHoja = pd.read_excel(file_path_desagregado, sheet_name=0, usecols=[0, 1, 3], skiprows=7)
        df_segundaHoja = pd.read_excel(file_path_desagregado, sheet_name=3, usecols=[2,6], skiprows=7)

        # Encuentra la fila en blanco y elimina las filas posteriores
        indice_fila_en_blanco = df_primeraHoja.index[df_primeraHoja.isnull().all(axis=1)].tolist()[0]
        df_primeraHoja = df_primeraHoja.iloc[:indice_fila_en_blanco]

        indice_fila_en_blanco = df_segundaHoja.index[df_segundaHoja.isnull().all(axis=1)].tolist()[0]
        df_segundaHoja = df_segundaHoja.iloc[:indice_fila_en_blanco]


        # Cargar el archivo Excel existente
        excel_file = file_path_destino
        sheet_name = 'Hoja1'

        with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            df_primeraHoja.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, startcol=0)
            df_encabezado.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
            df_segundaHoja.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, startcol=3)


import datetime
import time
import os
import pandas as pd

class loadXLSDataCBT:
    def readData(self):
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_destino = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')
        
        # Crear un DataFrame con las columnas de encabezado
        columnas_encabezado = [
            "Fecha",
            "CBA_Adulto",
            "CBT_Adulto",
            "CBA_Hogar",
            "CBT_Hogar",
            "CBA_NEA_Adulto",
            "CBT_NEA_Adulto",
            "CBA_NEA_Hogar",
            "CBT_NEA_Hogar"
        ]
        df_encabezado = pd.DataFrame(columns=columnas_encabezado)

        # Guardar el encabezado en el archivo de destino
        with pd.ExcelWriter(file_path_destino) as writer:
            df_encabezado.to_excel(writer, sheet_name='Hoja1', index=False)

        # Lee el archivo Excel y selecciona las columnas 1, 2 y 4 desde la fila 8
        df = pd.read_excel(file_path_desagregado, sheet_name=0, usecols=[0, 1, 3], skiprows=7)

        # Encuentra la fila en blanco y elimina las filas posteriores
        indice_fila_en_blanco = df.index[df.isnull().all(axis=1)].tolist()[0]
        df = df.iloc[:indice_fila_en_blanco]

        # Copiar los datos a un nuevo archivo Excel
        with pd.ExcelWriter(file_path_destino, mode='a', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Hoja1', index=False)

        
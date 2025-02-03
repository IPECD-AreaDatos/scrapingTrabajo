import pandas as pd
import os

class readSheets:
    def readDataDiccionario(self):
        #Fuente ------> https://docs.google.com/spreadsheets/d/15wcoRdv6oz1_uveUAyJJ6i-5vV3JIdN-/edit?gid=1323323809#gid=1323323809
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'DiccionarioClae.xlsx')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        
        df = pd.read_excel(file_path)
        return df

    def readDataPBG(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'PBG.csv')
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        
        df = pd.read_csv(file_path, sep=',')
        return df

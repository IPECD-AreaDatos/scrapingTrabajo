import pandas as pd
import os

class readSheets:
    def readDataPBG(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'PBG.csv')
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        
        df = pd.read_csv(file_path, sep=',')
        return df

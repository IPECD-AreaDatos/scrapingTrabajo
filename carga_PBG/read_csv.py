import pandas as pd
import os
class readSheets:
    def readData(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'PBG.csv')
        
        df = pd.read_csv(file_path, sep=',')
        
        return df
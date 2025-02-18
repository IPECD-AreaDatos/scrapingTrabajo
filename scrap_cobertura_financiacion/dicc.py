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
        print(df.dtypes)
        print(df)


Diccionario().construir_dicc()
#Transformaremos los datos para almacenar lo necesario en el datalake economico
import pandas as pd
import os

class Transform:

    def __init__(self):
        pass

    def extract_data_sheet(self):

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        #añadimos carpeta file
        path_folder_path = os.path.join(directorio_actual, 'files')

        #Añadimos direccion final del archivo del IPC de CABA
        path_xlsx = os.path.join(path_folder_path, 'ipc_caba.xlsx')


        #Creacion del DF y ajustamos
        df = pd.read_excel(path_xlsx,skiprows=4,usecols='F')
        df = df.dropna()
        df.columns = ["var_mensual_ipc_caba"]
        df = df.reset_index(drop=True)


        return df
from home_page import HomePage
from anac_armadoDF import armadoDF
import os
import pandas as pd


if __name__ == "__main__":
    #home_page = HomePage()
    #home_page.descargar_archivo()

    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'anac.xlsx')

    df = armadoDF.armadoDF(file_path_desagregado)
    print(df)
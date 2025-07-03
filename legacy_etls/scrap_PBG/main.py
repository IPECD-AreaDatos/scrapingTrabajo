import pdfplumber
import os
import sys
import pandas as pd 
import re  # Módulo para expresiones regulares
from readDataPDF import readDataPDF
from connectDBPrespuestoEjecutado import connectDBPrespuestoEjecutado
import glob 

#Configuracion de la ruta de credenciales
# Obtiene la ruta absoluta al directorio donde reside el script actual.
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


count = 0
valor_gastos = 0
valor_bienes = 1
valor_servicios = 2
valor_bien = 3

if __name__ == "__main__":
    # Usa una ruta general que sea independiente del sistema operativo
    carpeta_pdf = os.path.join(script_dir, 'files')  # Directorio de PDFs en la carpeta actual
    archivos_pdf = glob.glob(os.path.join(carpeta_pdf, '*.pdf'))  # Buscar todos los PDFs en la carpeta
    
    lector_pdf = readDataPDF()  # Crear una instancia de readDataPDF
    
    for ruta_pdf in archivos_pdf:
        df = lector_pdf.leer_datos(ruta_pdf, valor_gastos, valor_bienes, valor_servicios, valor_bien)
        instancia = connectDBPrespuestoEjecutado(host_dbb,user_dbb,pass_dbb,dbb_datalake)
        instancia.update_database_with_new_data(df)

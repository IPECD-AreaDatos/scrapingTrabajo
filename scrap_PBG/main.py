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
# Crea una ruta al directorio 'Credenciales_folder' que se supone está un nivel arriba en la jerarquía de directorios.
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')

count = 0
valor_gastos = 0
valor_bienes = 1
valor_servicios= 2
valor_bien = 3
if __name__ == "__main__":
    carpeta_pdf = "C:\\Users\\Matias\\Desktop\\scrapingTrabajo\\scrap_PBG\\files"
    archivos_pdf = glob.glob(carpeta_pdf + '/*.pdf')
    lector_pdf = readDataPDF()  # Crear una instancia de readDataPDF
    for ruta_pdf in archivos_pdf:
        df = lector_pdf.leer_datos(ruta_pdf, valor_gastos, valor_bienes, valor_servicios, valor_bien)
        instancia = connectDBPrespuestoEjecutado(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
        instancia.update_database_with_new_data(df)
    #pdf_path = "C:\\Users\\Matias\\Desktop\\scrapingTrabajo\\scrap_PBG\\files\\rf667 31 03 2023.pdf"
    #df = readDataPDF().leer_datos(pdf_path, valor_gastos, valor_bienes, valor_servicios, valor_bien)
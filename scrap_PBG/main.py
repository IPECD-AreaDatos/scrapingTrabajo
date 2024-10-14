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
valor_servicios = 2
valor_bien = 3

if __name__ == "__main__":
    # Usa una ruta general que sea independiente del sistema operativo
    carpeta_pdf = os.path.join(script_dir, 'files')  # Directorio de PDFs en la carpeta actual
    archivos_pdf = glob.glob(os.path.join(carpeta_pdf, '*.pdf'))  # Buscar todos los PDFs en la carpeta
    
    lector_pdf = readDataPDF()  # Crear una instancia de readDataPDF
    
    for ruta_pdf in archivos_pdf:
        df = lector_pdf.leer_datos(ruta_pdf, valor_gastos, valor_bienes, valor_servicios, valor_bien)
        instancia = connectDBPrespuestoEjecutado(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
        instancia.update_database_with_new_data(df)

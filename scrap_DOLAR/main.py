"""
En este script lo que vamos a hacer es rescatar los valores de distintos tipos de dolares:

- Dolar oficial: de banco nacion
- Dolar MEP, BLUE y CCL: sacado de https://www.ambito.com/ --> Sitio web dedicado al ambito bursatil
"""

from dolarOficial import dolarOficial
import os
import sys
import pandas as pd
from datetime import datetime

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')

from dolarBlue import dolarBlue
from dolarMEP import dolarMEP
from dolarCCL import dolarCCL

# Definir el rango de fechas
fecha_inicio = "01/01/2003"
fecha_fin = "31/12/2003"

# Convertir a objetos datetime
start_date = datetime.strptime(fecha_inicio, "%d/%m/%Y")
end_date = datetime.strptime(fecha_fin, "%d/%m/%Y")

if __name__ == '__main__':
    # dolarOficial().descargaArchivo()
    # dolarOficial().lecturaDolarOficial(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    
    try:
        # Llamar a la función tomaDolarBlue para el rango de fechas
        print(f"Ejecutando tomaDolarBlue para el rango de fechas: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}")
        db_scraper = dolarBlue(credenciales.host, credenciales.user, credenciales.password, credenciales.database, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)
        db_scraper.tomaDolarBlue()
        print(f"Datos del dólar blue obtenidos para el rango de fechas: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}.")
    except Exception as e:
        print(f"Error al obtener datos del dólar blue para el rango de fechas: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}: {e}")
    
    # dolarMEP().tomaDolarMEP(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    # dolarCCL().tomaDolarCCL(credenciales.host, credenciales.user, credenciales.password, credenciales.database)

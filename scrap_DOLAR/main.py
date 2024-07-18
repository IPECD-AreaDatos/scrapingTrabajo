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
fecha_inicio = "01/06/2020"
fecha_fin = "01/07/2024"  # Usar el primer día del mes siguiente

# Convertir a objetos datetime
start_date = datetime.strptime(fecha_inicio, "%d/%m/%Y")
end_date = datetime.strptime(fecha_fin, "%d/%m/%Y")

# Crear un rango de fechas mensual
dates = pd.date_range(start=start_date, end=end_date, freq='MS')

if __name__ == '__main__':
    # dolarOficial().descargaArchivo()
    # dolarOficial().lecturaDolarOficial(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    
    for date in dates:
        # Obtener el primer día del mes
        first_day_of_month = date.strftime("%d/%m/%Y")
        # Obtener el último día del mes
        last_day_of_month = (date + pd.offsets.MonthEnd()).strftime("%d/%m/%Y")
        
        try:
            # Llamar a la función tomaDolarBlue para el rango de fechas del mes actual
            print(f"Ejecutando tomaDolarBlue para el mes: {first_day_of_month} - {last_day_of_month}")
            dolarBlue(credenciales.host, credenciales.user, credenciales.password, credenciales.database, fecha_inicio=first_day_of_month, fecha_fin=last_day_of_month).tomaDolarBlue()
            print(f"Datos del dólar blue para el mes: {first_day_of_month} - {last_day_of_month} obtenidos.")
        except Exception as e:
            print(f"Error al obtener datos del dólar blue para el mes: {first_day_of_month} - {last_day_of_month}: {e}")
    
    # dolarMEP().tomaDolarMEP(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    # dolarCCL().tomaDolarCCL(credenciales.host, credenciales.user, credenciales.password, credenciales.database)

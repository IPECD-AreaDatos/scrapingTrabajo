"""
En este script lo que vamos a hacer es rescatar los valores de distintos tipos de dolares:

- Dolar oficial: de banco nacion
- Dolar MEP, BLUE y CCL: sacado de https://www.ambito.com/ --> Sitio web dedicado al ambito bursatil

"""

from dolarOficial import dolarOficial
import os
import sys 
# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
from credenciales_bdd import Credenciales
from dolarBlue import dolarBlue
from dolarMEP import dolarMEP
from dolarCCL import dolarCCL



credenciales = Credenciales()

print(credenciales.host,credenciales.user,credenciales.password,credenciales.database)

host = str(credenciales.host)
user = str(credenciales.user)
password = str(credenciales.password)
database = str(credenciales.database)


if __name__ == '__main__': 
    
    instancia_dolar = dolarOficial(credenciales.host,credenciales.user,credenciales.password,credenciales.database)
    instancia_dolar.descargaArchivo()
    instancia_dolar.lecturaDolarOficial()
    #dolarBlue().tomaDolarBlue()
    #dolarMEP().tomaDolarMEP()
    #dolarCCL().tomaDolarCCL()
    
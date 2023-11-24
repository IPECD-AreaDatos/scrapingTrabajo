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

if __name__ == '__main__': 
    #dolarOficial().descargaArchivo()
    #dolarOficial().lecturaDolarOficial(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #dolarBlue().tomaDolarBlue()
    dolarMEP().tomaDolarMEP()
    dolarCCL().tomaDolarCCL()
    
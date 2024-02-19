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
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('ipecd_economico')
from dolarBlue import dolarBlue
from dolarMEP import dolarMEP
from dolarCCL import dolarCCL


if __name__ == '__main__': 
    dolarOficial().descargaArchivo()
    dolarOficial().lecturaDolarOficial(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    dolarBlue().tomaDolarBlue(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    dolarMEP().tomaDolarMEP(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    dolarCCL().tomaDolarCCL(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
from extract import HomePage
import os
import pandas as pd
from ripte_cargaHistorico import ripte_cargaHistorico
from ripte_cargaUltimoDato import ripte_cargaUltimoDato
import sys
import os

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales()

#Instancia encargada 

if __name__ == '__main__':
    #Obtencion del archivo
    #home_page = HomePage()
    #home_page.descargar_archivo()
    #ripte_cargaHistorico().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    
    instancia = ripte_cargaUltimoDato(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia.loadInDataBase()
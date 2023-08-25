from homePage import HomePage
import os
import pandas as pd
from ripte_cargaHistorico import ripte_cargaHistorico
from ripte_cargaUltimoDato import ripte_cargaUltimoDato

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    #Obtencion del archivo
    #home_page = HomePage()
    #home_page.descargar_archivo()
    ripte_cargaHistorico().loadInDataBase(host, user, password, database)
    ripte_cargaUltimoDato().loadInDataBase(host, user, password, database)
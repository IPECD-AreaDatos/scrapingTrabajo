from homePage import HomePage
import os
import pandas as pd
from ripte_cargaHistorico import ripte_cargaHistorico
from ripte_cargaUltimoDato import ripte_cargaUltimoDato

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

#Instancia encargada 

if __name__ == '__main__':
    #Obtencion del archivo
    #home_page = HomePage()
    #home_page.descargar_archivo()
    #ripte_cargaHistorico().loadInDataBase(host, user, password, database)
    
    instancia = ripte_cargaUltimoDato(host, user, password, database)
    instancia.loadInDataBase()
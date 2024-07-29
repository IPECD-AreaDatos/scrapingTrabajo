"""
En este script lo que vamos a hacer es rescatar los valores de distintos tipos de dolares:

- Dolar oficial: de banco nacion
- Dolar MEP, BLUE y CCL: sacado de https://www.ambito.com/ --> Sitio web dedicado al ambito bursatil

"""

import os
import sys
from datetime import datetime

# Cargar las clases desde Ambito Financiero
from dolarOficial import dolarOficial
from dolarBlue import dolarBlue
from dolarMEP import dolarMEP
from dolarCCL import dolarCCL
# Cargar manual desde el excel
from dolarBlueHistorico import dolarBlueHistorico

# Carga de datos desde Dolarito
from readDataDolarOficial import readDataDolarOficial
from readDataDolarBlue import readDataDolarBlue
from readDataDolarMEP import readDataDolarMEP
from readDataDolarCCL import readDataDolarCCL

# Carga de correos
from sendMail import SendMail

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')


if __name__ == '__main__': 
    """Carga vieja desde Ambito Financiero"""
    #dolarOficial().descargaArchivo()
    #dolarOficial().lecturaDolarOficial(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #dolarBlue(credenciales.host, credenciales.user, credenciales.password, credenciales.database).tomaDolarBlue()
    #dolarMEP().tomaDolarMEP(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #dolarCCL().tomaDolarCCL(credenciales.host, credenciales.user, credenciales.password, credenciales.database)

    """Nueva carga desde dolarito"""
    #Dolar Oficial
    dolarOficial_reader = readDataDolarOficial(credenciales.user, credenciales.password, credenciales.host, credenciales.database, 'dolar_oficial')
    df_dolarOficial = dolarOficial_reader.readDataWebPage()
    dolarOficial_reader.insertDataFrameInDataBase(df_dolarOficial)
    #Dolar Blue
    dolarBlue_reader = readDataDolarBlue(credenciales.user, credenciales.password, credenciales.host, credenciales.database, 'dolar_blue')
    df_dolarBlue = dolarBlue_reader.readDataWebPage()
    dolarBlue_reader.insertDataFrameInDataBase(df_dolarBlue)
    #Dolar MEP
    dolarMEP_reader = readDataDolarMEP(credenciales.user, credenciales.password, credenciales.host, credenciales.database, 'dolar_mep')
    df_dolarMEP = dolarMEP_reader.readDataWebPage()
    dolarMEP_reader.insertDataFrameInDataBase(df_dolarMEP)
    #Dolar CCL
    dolarCCL_reader = readDataDolarCCL(credenciales.user, credenciales.password, credenciales.host, credenciales.database, 'dolar_ccl')
    df_dolarCCL = dolarCCL_reader.readDataWebPage()
    dolarCCL_reader.insertDataFrameInDataBase(df_dolarCCL)

    #Envio de correos
    send_mail= SendMail(credenciales.user, credenciales.password, credenciales.host, credenciales.database)
    df_dolarOficial, df_dolarBlue, df_dolarMEP, df_dolarCCL = send_mail.extract_data()
    send_mail.send_mail(df_dolarOficial, df_dolarBlue, df_dolarMEP, df_dolarCCL)

    """Carga directa desde el Excel(CAMBIAR NOMBRE DE LA TABLA)"""
    #dolarBlueHistorico(credenciales.user, credenciales.password, credenciales.host, credenciales.database, 'dolar_ccl').load_xlsx_to_mysql()
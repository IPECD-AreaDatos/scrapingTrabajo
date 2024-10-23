from downloadArchive import downloadArchive
from readFile import readFile
from uploadDataInDataBase import uploadDataInDataBase
from downloadArchive import downloadArchive
from readFile2 import readFile2
import os
import sys

#  Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__=='__main__':
    downloadArchive().descargar_archivo()
    df = readFile().read_file()
    df2 = readFile2().create_df()
    credenciales = uploadDataInDataBase(host_dbb,user_dbb,pass_dbb,dbb_datalake).conectar_bdd()
    credenciales.cargaBaseDatos(df2)
    credenciales.load_data(df)
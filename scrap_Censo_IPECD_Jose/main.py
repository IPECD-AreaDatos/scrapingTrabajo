import os
import sys
from readSheetAgua import readSheet
from conexionBaseDatos import conexionBaseDatos

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DATALAKE_SOCIO'))

if __name__ == "__main__":
    conexion = conexionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_dwh).conectar_bdd()
    
    df_agua = readSheet().read_data_agua()
    conexion.cargaBaseDatos_agua(df_agua)
    
    df_cloaca = readSheet().read_data_cloaca()
    conexion.cargaBaseDatos_cloaca(df_cloaca)
    
    df_combustible = readSheet().read_data_combustible()
    conexion.cargaBaseDatos_combustible(df_combustible)    
    
    df_inmat = readSheet().read_data_inmat()
    conexion.cargaBaseDatos_inmat(df_inmat)    
    
    df_internet = readSheet().read_data_internet()
    conexion.cargaBaseDatos_internet(df_internet)    
    
    df_material_de_piso = readSheet().read_data_material_de_piso()
    conexion.cargaBaseDatos_material_piso(df_material_de_piso)    
    
    df_nbi = readSheet().read_data_nbi()
    conexion.cargaBaseDatos_nbi(df_nbi)    
    
    df_p18_corrientes = readSheet().read_data_p18_corrientes()
    conexion.cargaBaseDatos_p18_corrientes(df_p18_corrientes)    
    
    df_poblacion_viviendas = readSheet().read_data_poblacion_viviendas()
    conexion.cargaBaseDatos_poblacion_viviendas(df_poblacion_viviendas)    
    
    df_propiedad_de_la_vivienda = readSheet().read_data_propiedad_de_la_vivienda()
    conexion.cargaBaseDatos_propiedad_vivienda(df_propiedad_de_la_vivienda)    
    
    df_tenencia_de_agua = readSheet().read_data_tenencia_de_agua()
    conexion.cargaBaseDatos_tenencia_agua(df_tenencia_de_agua)    
    
    conexion.cerrar_conexion()

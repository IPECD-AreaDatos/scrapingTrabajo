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

    #Se agrego el 19/11

    # Crear y cargar datos de asistencia escolar
    df_asistencia_escolar = readSheet().read_data_asistencia_escolar()
    conexion.cargaBaseDatos_asistencia_escolar(df_asistencia_escolar)

    # Crear y cargar datos de categoría ocupacional
    df_categoria_ocupacional = readSheet().read_data_categoria_ocupacional()
    conexion.cargaBaseDatos_categoria_ocupacional(df_categoria_ocupacional)

    # Crear y cargar datos de cobertura de salud
    df_cobertura_salud = readSheet().read_data_cobertura_salud()
    conexion.cargaBaseDatos_cobertura_salud(df_cobertura_salud)

    # Crear y cargar datos de nivel educativo mayores de 25
    df_nivel_educativo_mayores_25 = readSheet().read_data_nivel_educativo_mayores_veinticico()
    conexion.cargaBaseDatos_nivel_educativo_mayores_25(df_nivel_educativo_mayores_25)

    # Crear y cargar datos de tasas del mercado de trabajo
    df_tasas_mercado_trabajo = readSheet().read_data_tasa_mercado_de_trabajo()
    conexion.cargaBaseDatos_tasas_mercado_trabajo(df_tasas_mercado_trabajo)
    

    #Se agrego el 22/11

    df_clima_educativo_municipios_2022 = readSheet().read_data_clima_educativo_municipio_2022()
    conexion.cargarBaseDatos_clima_educativo_municipio_2022(df_clima_educativo_municipios_2022)
 

    
    conexion.cerrar_conexion()

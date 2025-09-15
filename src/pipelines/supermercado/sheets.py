import logging
import os
import pandas as pd
import decimal
import numpy as np
from src.shared.sheets import ConexionGoogleSheets
from src.shared.db import ConexionBaseDatos
from dotenv import load_dotenv

def load_supermercado_sheets_data(df_deflactado, datos_nuevos):
    logger = logging.getLogger("supermercado")
    logger.info("💾 Iniciando carga al Google Sheets...")

    if not datos_nuevos:
        logger.info("⚠️ No hay datos nuevos para cargar al Sheets.")
        print("⛔ datos_nuevos es False")
        return
    
    lista_deflactada = df_deflactado.loc[
        (df_deflactado['id_provincia_indec'] == 18) & 
        (df_deflactado['fecha'] >= '2018-12-01'),
        'total_facturacion'
    ].tolist()
    print("Lista para cargar al Sheets:")
    print(lista_deflactada)

    SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
    sheets = ConexionGoogleSheets(SPREADSHEET_ID)

    # Rango donde vas a escribir (fila 11, columnas desde C en adelante)
    rango = 'Datos!C11:11'  

    try:
        # IMPORTANTE: values tiene que ser lista de listas, donde cada sublista es una fila
        sheets.escribir_rango(rango, [lista_deflactada])  
        logger.info("✅ Carga a Google Sheets completada.")
        print("✅ Valores escritos en Google Sheets.")
    except Exception as e:
        print(f"❌ Excepción: {e}")
        logger.error(f"❌ Error durante la carga a Google Sheets: {e}")


def deflactador_supermercado_data(df, db: ConexionBaseDatos):
    logger = logging.getLogger("supermercado")
    logger.info("💾 Iniciando deflactacion...")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    
    df_ipc_region = get_ipc_data(df,host, user, password, database)
    df_datos_todas_las_provincias = agrupar_datos(df,df_ipc_region)
    df_deflactado = calculo_deflactacion(df_datos_todas_las_provincias)

    return df_deflactado

def load_supermercado_deflactado_data(df, db: ConexionBaseDatos):
    logger = logging.getLogger("supermercado")
    logger.info("💾 Iniciando carga de deflactacion a la base...")

    exito = db.load_if_newer(
        df,
        table_name="supermercado_deflactado",
        date_column="fecha"
    )

    return exito

def calculo_deflactacion(df_datos_todas_las_provincias):


    def convertir_decimal_a_float(x):
        if isinstance(x, decimal.Decimal):
            return float(x)
        try:
            return float(x)
        except Exception:
            return np.nan

    def preparar_df_para_deflactacion(df):
        df = df.copy()
        # Columnas que no se convierten (fecha, ids)
        columnas_no_convertir = ['fecha', 'id_region_indec', 'id_provincia_indec']
        columnas_convertir = [col for col in df.columns if col not in columnas_no_convertir]

        for col in columnas_convertir:
            df[col] = df[col].apply(convertir_decimal_a_float)

        # Verificamos si quedan valores decimal.Decimal
        for col in columnas_convertir:
            if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                print(f"Quedan valores decimal.Decimal en la columna: {col}")

        return df

    df = preparar_df_para_deflactacion(df_datos_todas_las_provincias)

    df_deflactado = pd.DataFrame()
    df_deflactado['fecha'] = df['fecha']
    df_deflactado['id_region_indec'] = df['id_region_indec']
    df_deflactado['id_provincia_indec'] = df['id_provincia_indec']

    # === Calculo con ponderaciones asignadas

    df_deflactado['total_facturacion'] = df['total_facturacion'] / (df['general'] / 100)

    df_deflactado['bebidas'] = df['bebidas'] / (((df['aguasminerales_bebidas_gaseosas'] * 0.5) + (df['bebidas_alcholicas'] * 0.5)) / 100)

    df_deflactado['almacen'] = df['almacen'] / (((df['pan_cereales'] * (1/3)) + (df['aceite_grasas_mantecas'] * (1/3)) + (df['azucar_chocalate_golosina'] * (1/3))) / 100)

    df_deflactado['panaderia'] = df['panaderia'] / (df['pan_cereales'] / 100)

    df_deflactado['lacteos'] = df['lacteos'] / (df['leche_productos_lacteos_huevos'] / 100)

    df_deflactado['carnes'] = df['carnes'] / (df['carnes_derivados'] / 100)

    df_deflactado['verduleria_fruteria'] = df['verduleria_fruteria'] / (df['verduras_tuberculos_legumbres'] / 100)

    df_deflactado['alimentos_preparados_rostiseria'] = df['alimentos_preparados_rostiseria'] / (df['restaurantes_comida_fueradelhogar'] / 100)

    df_deflactado['articulos_limpieza_perfumeria'] = df['articulos_limpieza_perfumeria'] / (((df['bienes_servicios_conservacionhogar'] * 0.5) + (df['cuidadopersonal'] * 0.5)) / 100)

    df_deflactado['indumentaria_calzado_textiles_hogar'] = df['indumentaria_calzado_textiles_hogar'] / (df['prendasdevestir_y_calzados'] / 100)

    df_deflactado['electronica_hogar'] = df['electronica_hogar'] / (df['equipos_audivisuales_fotograficos_procesamiento_info'] / 100)

    df_deflactado['otros'] = df['otros'] / (df['general'] / 100)

    return df_deflactado


def agrupar_datos(df,df_ipc_region):
    #Obtenemos en forma de LISTA los ID de la provincias
    id_provincias = list(pd.unique(df['id_provincia_indec']))

    #Eliminamos NACION ya que es un dato que no usaremos. Tambien la PROV de buenos aires ya que tiene un trato particular
    id_provincias.remove(1)
    id_provincias.remove(6)

    #Agrupamos las columnas que vamos a usar - Es un conjunto de las tablas del supermercado y de la IPC y los valores por region
    lista_columnas = list(df.columns)

    for columnas in df_ipc_region.columns:

        if columnas == 'id_region_indec':  #--> Se elimina para que no existan 2 columnas con el mismo valor
            pass
        else:
            lista_columnas.append(columnas)

    # === CREACION DE DATAFRAMES QUE TRATAREMOS

    df_datos_todas_las_provincias = pd.DataFrame(columns=lista_columnas) #--> DF con el que concatenamos datos
    df_datos_por_provincia  = pd.DataFrame(columns=lista_columnas)#--> DF que contiene los datos por provincia
    df_buenos_aires = data_buenos_aires(lista_columnas,df,df_ipc_region)#--> #DF que contiene los datos de BUENOS AIRES

    #Concatenamos los datos de BSAR con el de todas las provincias
    df_datos_todas_las_provincias = pd.concat([df_datos_todas_las_provincias,df_buenos_aires])

    # === AGRUPACION DE TODAS LAS RESTANTES PROVINCIAS

    #Recorremos todas las provincias por su ID
    for id_provincia in id_provincias:

        #Obtencion de la totalidad de los datos por provincia de las encuestas del supermercado
        for columna_supermercado in (df.columns):

            df_datos_por_provincia[columna_supermercado] = list(df[columna_supermercado]
                                                                    [df['id_provincia_indec'] == id_provincia])

        #=== Obtencion de la totalidad de datos del IPC por region

        #Obtencion de la region
        region = df_datos_por_provincia['id_region_indec'].values[0]

        #Obtencion de los datos de dicha region
        df_region_buscada = df_ipc_region[df_ipc_region['id_region_indec'] == region]

        #Asignamos los datos por subdivision
        for columna_ipc_subdivision in df_region_buscada.columns:

            df_datos_por_provincia[columna_ipc_subdivision] = df_region_buscada[columna_ipc_subdivision]


        #Concatenamos los datos de la provincia actual al DF total
        df_datos_todas_las_provincias = pd.concat([df_datos_todas_las_provincias,df_datos_por_provincia])
            
    return df_datos_todas_las_provincias

def data_buenos_aires(lista_columnas,df,df_ipc_region):

    #=== Estudia de BUENOS AIRES

    #Obtenemos datos de BUENOS AIRES en la REGION 2 y REGION 3
    df_datos_por_provincia_gba = pd.DataFrame()
    df_datos_por_provincia_pampeana= pd.DataFrame()
    df_buenos_aires= pd.DataFrame(columns=lista_columnas)

    #datos de los supermeracos

    for columna_sm in df.columns:

        df_datos_por_provincia_gba[columna_sm] = list(df[columna_sm][(df['id_provincia_indec'] == 6) & (df['id_region_indec'] == 2)])
        df_datos_por_provincia_pampeana[columna_sm] = list(df[columna_sm][(df['id_provincia_indec'] == 6) & (df['id_region_indec'] == 3)])
                

    #Lista sin id_region_indec
    lista_sin_region = list(df_ipc_region.columns)
    lista_sin_region.remove('id_region_indec')

    for columna_ipc in lista_sin_region:

        #Agregamos los datos por region de IPC a la tabla
        df_datos_por_provincia_gba[columna_ipc] = list(df_ipc_region[columna_ipc][df_ipc_region['id_region_indec'] == 2])
        df_datos_por_provincia_pampeana[columna_ipc] = list(df_ipc_region[columna_ipc][df_ipc_region['id_region_indec'] == 3])
        

    #Datos de BS concatenados
    df_buenos_aires = pd.concat([df_buenos_aires,df_datos_por_provincia_gba,df_datos_por_provincia_pampeana])
    
    return df_buenos_aires

def get_ipc_data(df, host, user, password, database):

    db = ConexionBaseDatos(host, user, password, database)
    db.connect_db() 

    fecha_min = min(df['fecha'])
    fecha_max = max(df['fecha'])

    df_ipc = db.read_sql_between_dates('ipc_valores', fecha_min, fecha_max)

    #Columnas del IPC por subdivision --> No estan todas, sino solo las que vamos a ocupar
    columnas = ['id_region_indec','general','aguasminerales_bebidas_gaseosas','bebidas_alcholicas','pan_cereales','leche_productos_lacteos_huevos','carnes_derivados','verduras_tuberculos_legumbres',
        'alimentos','restaurantes_comida_fueradelhogar','aceite_grasas_mantecas','azucar_chocalate_golosina','bienes_servicios_conservacionhogar','cuidadopersonal',
        'prendasdevestir_y_calzados','equipos_audivisuales_fotograficos_procesamiento_info']

    #Datos del IPC por subdivision --> En este DF vamos a agrupar los datos
    df_total_ipc = pd.DataFrame(columns = columnas)

    #Datos que obtenemos por region, una vez obtenidos los concatenamos a df_total_ipc
    df_datopor_region = pd.DataFrame(columns=columnas)

    #Regiones por ID
    regiones = [2,3,4,5,6,7]

    print("Dimensiones de df_ipc:", df_ipc.shape)
    print("Dimensiones de df_datopor_region:", df_datopor_region.shape)
    print("Dimensiones de df_total_ipc:", df_total_ipc.shape)

    for region in regiones:

            df_datopor_region['general'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 1) & (df_ipc['id_region'] == region)])
            df_datopor_region['aguasminerales_bebidas_gaseosas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 13) & (df_ipc['id_region'] == region)])
            df_datopor_region['bebidas_alcholicas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 15) & (df_ipc['id_region'] == region)])
            df_datopor_region['pan_cereales'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 4) & (df_ipc['id_region'] == region)])
            df_datopor_region['leche_productos_lacteos_huevos'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 6) & (df_ipc['id_region'] == region)])
            df_datopor_region['carnes_derivados'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 5) & (df_ipc['id_region'] == region)])
            df_datopor_region['verduras_tuberculos_legumbres'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 9) & (df_ipc['id_region'] == region)])
            df_datopor_region['alimentos'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 3) & (df_ipc['id_region'] == region)])
            print(f"Procesando región: {region}")
            general_values = df_ipc['valor'][(df_ipc['id_subdivision'] == 1) & (df_ipc['id_region'] == region)]
            print(f"Valores para 'general' en región {region}: {general_values}")
            df_datopor_region['aceite_grasas_mantecas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==7) & (df_ipc['id_region'] == region)])
            df_datopor_region['azucar_chocalate_golosina'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==10) & (df_ipc['id_region'] == region)])
            df_datopor_region['bienes_servicios_conservacionhogar'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==26) & (df_ipc['id_region'] == region)])
            print(f"Procesando región: {region}")
            cuidadopersonal_values = df_ipc['valor'][(df_ipc['id_subdivision'] == 45) & (df_ipc['id_region'] == region)]
            print(f"Valores para 'cuidadopersonal' en región {region}: {cuidadopersonal_values}")
            df_datopor_region['prendasdevestir_y_calzados'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==17) & (df_ipc['id_region'] == region)])

            #Esta subdivision solo existe en GBA, entonces se replica para cada region. id de GBA = 2 y la subdivision = 38
            df_datopor_region['equipos_audivisuales_fotograficos_procesamiento_info'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==38) & (df_ipc['id_region'] == 2)])

            #La region se asigna ultimo ya que los las primeras filas se define el tamaño del DF por region, una vez definido asignamos el mismo valor a multiples filas
            df_datopor_region['id_region_indec'] = region

            #Concatenacion del df_total, con los datos obtenidos por region
            df_total_ipc = pd.concat([df_total_ipc,df_datopor_region])


    return df_total_ipc

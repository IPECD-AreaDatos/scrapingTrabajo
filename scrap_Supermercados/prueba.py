import mysql.connector
import pandas as pd
import sys

host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'
database = 'datalake_economico'

conn = mysql.connector.connect(
    host=host, user=user, password=password, database=database
)
cursor = conn.cursor()

#Extraemos datos del SUPERMERCADO
query_supemercado = "SELECT * FROM supermercado_encuesta"
df_supermercado = pd.read_sql(query_supemercado,conn)
df_supermercado = df_supermercado.sort_values(by=['id_region_indec','id_provincia_indec','fecha'])

#Buscamos la fecha maxima y la fecha minima para truncar por ese rango de fechas la tabla de IPC
fecha_min = min(df_supermercado['fecha'])
fecha_max = max(df_supermercado['fecha'])

print(df_supermercado)


#Extramos los datos del IPC solo hasta la fecha maxima solicitada
query_ipc = f"SELECT * FROM ipc_valores WHERE fecha BETWEEN '{fecha_min}' AND '{fecha_max}';"
df_ipc = pd.read_sql(query_ipc,conn)

#Columnas del IPC por subdivision
columnas = ['id_region_indec','general','aguasminerales_bebidas_gaseosas','bebidas_alcholicas','pan_cereales','leche_productos_lacteos_huevos','carnes_derivados','verduras_tuberculos_legumbres',
'alimentos','restaurantes_comida_fueradelhogar','aceite_grasas_mantecas','azucar_chocalate_golosina','bienes_servicios_conservacionhogar','cuidadopersonal',
'prendasdevestir_y_calzados','equipos_audivisuales_fotograficos_procesamiento_info']

#Datos del IPC por subdivision
df_total = pd.DataFrame(columns = columnas)
df_datopor_region = pd.DataFrame(columns=columnas)

regiones = [2,3,4,5,6,7]

for region in regiones:

    df_datopor_region['general'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 1) & (df_ipc['id_region'] == region)])
    df_datopor_region['aguasminerales_bebidas_gaseosas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 13) & (df_ipc['id_region'] == region)])
    df_datopor_region['bebidas_alcholicas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 15) & (df_ipc['id_region'] == region)])
    df_datopor_region['pan_cereales'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 4) & (df_ipc['id_region'] == region)])
    df_datopor_region['leche_productos_lacteos_huevos'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 6) & (df_ipc['id_region'] == region)])
    df_datopor_region['carnes_derivados'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 5) & (df_ipc['id_region'] == region)])
    df_datopor_region['verduras_tuberculos_legumbres'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 9) & (df_ipc['id_region'] == region)])
    df_datopor_region['alimentos'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 3) & (df_ipc['id_region'] == region)])
    df_datopor_region['restaurantes_comida_fueradelhogar'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] == 43) & (df_ipc['id_region'] == region)])
    df_datopor_region['aceite_grasas_mantecas'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==7) & (df_ipc['id_region'] == region)])
    df_datopor_region['azucar_chocalate_golosina'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==10) & (df_ipc['id_region'] == region)])
    df_datopor_region['bienes_servicios_conservacionhogar'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==26) & (df_ipc['id_region'] == region)])
    df_datopor_region['cuidadopersonal'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==45) & (df_ipc['id_region'] == region)])
    df_datopor_region['prendasdevestir_y_calzados'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==17) & (df_ipc['id_region'] == region)])

    #Esta subdivision solo existe en GBA, entonces se replica para cada region. id de GBA = 2 y la subdivision = 38
    df_datopor_region['equipos_audivisuales_fotograficos_procesamiento_info'] = list(df_ipc['valor'][(df_ipc['id_subdivision'] ==38) & (df_ipc['id_region'] == 2)])

    #La region se asigna ultimo ya que los las primeras filas se define el tamaño del DF por region, una vez definido asignamos el mismo valor a multiples filas
    df_datopor_region['id_region_indec'] = region

    #Concatenacion del df_total, con los datos obtenidos por region
    df_total = pd.concat([df_total,df_datopor_region])


#Obtenemos en forma de LISTA los ID de la provincias
id_provincias = list(pd.unique(df_supermercado['id_provincia_indec']))

#Eliminamos NACION ya que es un dato que no usaremos. Tambien la PROV de buenos aires ya que tiene un trato particular
id_provincias.remove(1)
id_provincias.remove(6)

#Agrupamos las columnas que vamos a usar - Es un conjunto de las tablas del supermercado y de la IPC y los valores por region
lista_columnas = list(df_supermercado.columns)

for columnas in df_total.columns:
    lista_columnas.append(columnas)


#Df que contendra todos los datos
df_datos_super_deflactados = pd.DataFrame(columns=lista_columnas)
df_datos_por_provincia = pd.DataFrame(columns=lista_columnas)

for id_provincia in id_provincias:

    #Obtencion de la totalidad de los datos POR PROVINCIA
    df_datos_por_provincia[df_supermercado.columns] = df_supermercado
    [df_supermercado.columns]
    [df_supermercado['id_provincia_indec'] == id_provincia]

    print(df_datos_por_provincia)
    pass
    #Obtencion de la region
    #region = pd.unique(df_datos_por_provincia['id_region_indec'])[0]

    #df_datos_por_provincia['general'] = df_total['general'][df_total['id_region_indec'] == region]

    #print(df_datos_por_provincia['general'])


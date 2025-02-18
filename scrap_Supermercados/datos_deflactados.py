#Objetivo: Lograr una deflactacion de las encuentas de los supermercados, con el objetivo de comparar
#Las encuentas de multiples provincias
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from googleapiclient.discovery import build
import pandas as pd
import sys
import os
from google.oauth2 import service_account
from json import loads

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

class Deflactador:

    def __init__(self, host, user, password, database):
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

        

    #Conexion a la base de datos
    def connect_bdd(self):

        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )

        self.cursor = self.conn.cursor()

    #Objetivo: Obtener los datos del IPC por region
    def get_data_ipc(self,fecha_min,fecha_max):

        #Extramos los datos del IPC solo hasta la fecha maxima solicitada
        query_ipc = f"SELECT * FROM ipc_valores WHERE fecha BETWEEN '{fecha_min}' AND '{fecha_max}';"
        df_ipc = pd.read_sql(query_ipc,self.conn)
        print("df ipc")
        print(df_ipc)
        print(fecha_min)

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
    
    #Objetivo: obtener los datos de los supermercados, y sus fechas maximas y minimas
    def get_data_supermercado(self):

        #Extraemos datos del SUPERMERCADO
        query_supemercado = "SELECT * FROM supermercado_encuesta"
        df_supermercado = pd.read_sql(query_supemercado,self.conn)
        df_supermercado = df_supermercado.sort_values(by=['id_region_indec','id_provincia_indec','fecha'])

        #Buscamos la fecha maxima y la fecha minima para truncar por ese rango de fechas la tabla de IPC
        fecha_min = min(df_supermercado['fecha'])
        fecha_max = max(df_supermercado['fecha'])

        return df_supermercado,fecha_min,fecha_max


    def data_buenos_aires(self,lista_columnas,df_supermercado,df_ipc_region):

        #=== Estudia de BUENOS AIRES

        #Obtenemos datos de BUENOS AIRES en la REGION 2 y REGION 3
        df_datos_por_provincia_gba = pd.DataFrame()
        df_datos_por_provincia_pampeana= pd.DataFrame()
        df_buenos_aires= pd.DataFrame(columns=lista_columnas)

        #datos de los supermeracos

        for columna_sm in df_supermercado.columns:

            df_datos_por_provincia_gba[columna_sm] = list(df_supermercado[columna_sm][(df_supermercado['id_provincia_indec'] == 6) & (df_supermercado['id_region_indec'] == 2)])
            df_datos_por_provincia_pampeana[columna_sm] = list(df_supermercado[columna_sm][(df_supermercado['id_provincia_indec'] == 6) & (df_supermercado['id_region_indec'] == 3)])
                

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

    #Objetivo: crear una aglomeracion de los datos de las encuestas de supermercado por provincia, y que 
    #estos datos esten acompañados por los valores de subdivision de IPC que lo representan
    def agrupar_datos(self,df_supermercado,df_ipc_region):

        #Obtenemos en forma de LISTA los ID de la provincias
        id_provincias = list(pd.unique(df_supermercado['id_provincia_indec']))

        #Eliminamos NACION ya que es un dato que no usaremos. Tambien la PROV de buenos aires ya que tiene un trato particular
        id_provincias.remove(1)
        id_provincias.remove(6)

        #Agrupamos las columnas que vamos a usar - Es un conjunto de las tablas del supermercado y de la IPC y los valores por region
        lista_columnas = list(df_supermercado.columns)

        for columnas in df_ipc_region.columns:

            if columnas == 'id_region_indec':  #--> Se elimina para que no existan 2 columnas con el mismo valor
                pass
            else:
                lista_columnas.append(columnas)

        # === CREACION DE DATAFRAMES QUE TRATAREMOS

        df_datos_todas_las_provincias = pd.DataFrame(columns=lista_columnas) #--> DF con el que concatenamos datos
        df_datos_por_provincia  = pd.DataFrame(columns=lista_columnas)#--> DF que contiene los datos por provincia
        df_buenos_aires = self.data_buenos_aires(lista_columnas,df_supermercado,df_ipc_region)#--> #DF que contiene los datos de BUENOS AIRES

        #Concatenamos los datos de BSAR con el de todas las provincias
        df_datos_todas_las_provincias = pd.concat([df_datos_todas_las_provincias,df_buenos_aires])

        # === AGRUPACION DE TODAS LAS RESTANTES PROVINCIAS

        #Recorremos todas las provincias por su ID
        for id_provincia in id_provincias:

            #Obtencion de la totalidad de los datos por provincia de las encuestas del supermercado
            for columna_supermercado in (df_supermercado.columns):

                df_datos_por_provincia[columna_supermercado] = list(df_supermercado[columna_supermercado]
                                                                    [df_supermercado['id_provincia_indec'] == id_provincia])

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

    def calculo_deflactacion(self,df_datos_todas_las_provincias):

        df_deflactado = pd.DataFrame()

        #Asignacion de columnas comunes
        df_deflactado['fecha'] = df_datos_todas_las_provincias['fecha']
        df_deflactado['id_region_indec'] = df_datos_todas_las_provincias['id_region_indec']
        df_deflactado['id_provincia_indec'] = df_datos_todas_las_provincias['id_provincia_indec']

        #=== Calculo con ponderaciones asignadas

        #Ponderacion general
        df_deflactado['total_facturacion'] = df_datos_todas_las_provincias['total_facturacion'] / (df_datos_todas_las_provincias['general'] / 100 )

        #Ponderacion de bebidas
        df_deflactado['bebidas'] = df_datos_todas_las_provincias['bebidas'] / (( (df_datos_todas_las_provincias['aguasminerales_bebidas_gaseosas'] * (1/2) ) + (df_datos_todas_las_provincias['bebidas_alcholicas'] * (1/2) )  ) /100 )

        #Ponderacion de almacen
        df_deflactado['almacen'] = df_datos_todas_las_provincias['almacen'] / (((df_datos_todas_las_provincias['pan_cereales'] * (1/3)) + (df_datos_todas_las_provincias['aceite_grasas_mantecas'] * (1/3)) + (df_datos_todas_las_provincias['azucar_chocalate_golosina'] * (1/3))) / 100)

        #Ponderacion de panaderia
        df_deflactado['panaderia'] = df_datos_todas_las_provincias['panaderia'] / (df_datos_todas_las_provincias['pan_cereales'] / 100)

        #Ponderacion de lacteos
        df_deflactado['lacteos'] = df_datos_todas_las_provincias['lacteos'] / (df_datos_todas_las_provincias['leche_productos_lacteos_huevos'] / 100)

        #Ponderacion de carnes
        df_deflactado['carnes'] = df_datos_todas_las_provincias['carnes'] / (df_datos_todas_las_provincias['carnes_derivados'] / 100)

        #Ponderacion de verduleria y fruteria
        df_deflactado['verduleria_fruteria'] = df_datos_todas_las_provincias['verduleria_fruteria'] / (df_datos_todas_las_provincias['verduras_tuberculos_legumbres'] / 100)

        #ponderacion de alimentos preparados y rostiseria
        df_deflactado['alimentos_preparados_rostiseria'] = df_datos_todas_las_provincias['alimentos_preparados_rostiseria'] / (df_datos_todas_las_provincias['restaurantes_comida_fueradelhogar'] / 100)

        #Ponderacion de articulos de limpieza
        df_deflactado['articulos_limpieza_perfumeria'] = df_datos_todas_las_provincias['articulos_limpieza_perfumeria'] / (((df_datos_todas_las_provincias['bienes_servicios_conservacionhogar'] * (1/2)) + (df_datos_todas_las_provincias['cuidadopersonal'] * (1/2))) /100 )

        #Ponderacion de Indumentaria, calzado y textiles
        df_deflactado['indumentaria_calzado_textiles_hogar'] = df_datos_todas_las_provincias['indumentaria_calzado_textiles_hogar'] / (df_datos_todas_las_provincias['prendasdevestir_y_calzados'] / 100 )

        #Ponderacion de Equipos audiovisuales, fotográficos y de procesamiento de la información
        df_deflactado['electronica_hogar'] = df_datos_todas_las_provincias['electronica_hogar'] /  (df_datos_todas_las_provincias['equipos_audivisuales_fotograficos_procesamiento_info'] / 100 )


        df_deflactado['otros'] = df_datos_todas_las_provincias['otros'] /  (df_datos_todas_las_provincias['general'] / 100 )

        return df_deflactado


    def cargar_datos(self,df_deflactado):

        delete_query ="TRUNCATE `dwh_economico`.`supermercado_deflactado`"
        self.cursor.execute(delete_query)
        print("tdv no")

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/dwh_economico")
        df_deflactado.to_sql(name="supermercado_deflactado", con=engine, if_exists='append', index=False)
        print("cargado")

    #Objetivo:main
    def main(self):
        
        self.connect_bdd()
        df_supermercado,fecha_min,fecha_max = self.get_data_supermercado()
        df_ipc_region = self.get_data_ipc(fecha_min,fecha_max)
        df_datos_todas_las_provincias = self.agrupar_datos(df_supermercado,df_ipc_region)
        df_deflactado = self.calculo_deflactacion(df_datos_todas_las_provincias)
        print(df_deflactado)
        self.cargar_datos(df_deflactado)
        print(f"Contenido de GOOGLE_SHEETS_API_KEY: {os.getenv('GOOGLE_SHEETS_API_KEY')}")
        self.save_data_sheet()
        print("sheet cargado")



    def save_data_sheet(self):
        query_select = 'SELECT total_facturacion FROM dwh_economico.supermercado_deflactado WHERE id_provincia_indec = 18 and fecha >= "2018-12-01"'
        df = pd.read_sql(query_select, self.conn)

        # Convierte el resultado a una lista de Python
        lista_deflactada = df['total_facturacion'].tolist()

        print(lista_deflactada)

        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))


        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()


        #Remplzamos los datos en la fila correspondiente
        request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C11:11',
                                        valueInputOption='RAW',
                                        body={'values':[lista_deflactada]}).execute()



#Deflactador(host = '54.94.131.196',user = 'estadistica',password = 'Estadistica2024!!',database = 'dwh_economico').main()





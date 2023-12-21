import mysql.connector
from pandas import isna

class conexionBaseDatos:

    def __init__(self, host, user, password, database):
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    #Conexion a la base de datos
    def conectar_bdd(self,host,user,password,database):

        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        self.cursor = self.conn.cursor()


    def cargar_datos(self,df):
        
        #Conectamos a la base de datos
        self.conectar_bdd(self.host,self.user,self.password,self.database)

        #Definimos querys que vamos a utilizar
        nombre_tabla = 'supermercado_encuesta'
        delete_query ="TRUNCATE `ipecd_economico`.`supermercado_encuesta`"
        query_cantidad_datos = f'SELECT COUNT(*) FROM {nombre_tabla}'
        query_insertar_datos = "INSERT INTO supermercado_encuesta VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)"

                
        #Si las cantidad de filas del DF descargado, es mayor que el ya almacenado --> Realizar carga
        if (self.validacion_de_carga(df,query_cantidad_datos)):

            #Eliminamos tabla
            self.cursor.execute(delete_query)

            #Iteramos el dataframe, y vamos cargando fila por fila
            for index,valor in df.iterrows():
                
                #Obtencion de valores del dataframe e insercion de datos
                values = (valor['provincia'],valor['fecha'],valor['bebidas'],valor['almacen'],valor['panaderia'],
                          valor['lacteos'],valor['carnes'],valor['verduleria_fruteria'],valor['alimentos_preparados_rostiseria'],
                          valor['articulo_limpieza_perfumeria'],valor['indumentaria_calzado_textiles_hogar'],valor['electronica_hogar'],
                          valor['otros']
                          )

                # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
                values = [None if isna(v) else v for v in values]
                
                #Insercion de dato fila por fila
                self.cursor.execute(query_insertar_datos,values)

            
            # Confirmar los cambios en la base de datos
            self.conn.commit()
            # Cerrar el cursor y la conexión
            self.cursor.close()
            self.conn.close()


            print("""

            ================================================================================
             *** SE HA PRODUCIDO UNA ACTUALIZACION EN LAS ENCUESTAS DE SUPERMERCADOS ***
            ================================================================================

            """)

        else:
            print(f"NO HAY CAMBIOS EN LOS DATOS DE {nombre_tabla}")
                

    
    #Realizamos una validacion de carga comparando el tamaño del dataframe con la cantidad de datos almacenados
    def validacion_de_carga(self,df,query_cantidad_datos):

        #Obtencion de tamaño del DF sacado del EXCEL
        cant_filas_df = len(df)

        #Obtencion de cantidad de filas de 
        self.cursor.execute(query_cantidad_datos)
        row_count_before = self.cursor.fetchone()[0]

        return (cant_filas_df > row_count_before)








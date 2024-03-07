import pandas as pd
from sqlalchemy import create_engine
import pymysql

class connection_db:

    def __init__(self,mysql_host,mysql_user, mysql_password ,database):

        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.database = database
        self.tunel = None
        self.conn = None
        self.cursor = None

    # =========================================================================================== #
                    # ==== SECCION CORRESPONDIENTE A SETTERS Y GETTERS ==== #
    # =========================================================================================== #
            
    #Objetivo: cambiar el nombre de la base de datos para reconectarnos a otra.
    def set_database(self,new_name):

        self.database = new_name


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #
        

    #Conexion a la BDD
    def connect_db(self):

            self.conn = pymysql.connect(
                host = self.mysql_host,
                user = self.mysql_user,
                password = self.mysql_password,
                database = self.database
            )

            self.cursor = self.conn.cursor()

    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()



# =========================================================================================== #
                # ==== SECCION CORRESPONDIENTE AL DATALAKE ==== #
# =========================================================================================== #
        


    #Objetivo: Almacenar los datos de CBA y CBT sin procesar en el datalake. Datos sin procesar
    def load_datalake(self,df):
        
        #Obtenemos los tamanios
        tamanio_df,tamanio_bdd = self.determinar_tamanios(df)

        if tamanio_df > tamanio_bdd: #Si el DF es mayor que lo almacenado, cargar los datos nuevos
            
            #Es necesario el borrado ya que posteriormente las estimaciones tendremos que recalcularlas
            delete_query = 'TRUNCATE cbt_cba'
            self.cursor.execute(delete_query)


            self.cargar_tabla_datalake(df) 
            print("==== SE CARGARON DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")

            #Nos vamos a reconectar al DWH de sociodemografico para enviar el correo
            print("Base de datos actual",self.database)
            self.table_a1()

            #Bandera que usaremos en main para enviar correo
            return True 
            
        else: #Si no hay datos nuevos AVISAR
            
            print("==== NO HAY DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")   
            
            #Bandera que usaremos en main para enviar correo
            return False  

    #Objetivo: Obtener tamaños de los datos para realizar verificaciones de varga
    def determinar_tamanios(self,df):

        #Obtenemos la cantidad de datos almacenados
        query_consulta = "SELECT COUNT(*) FROM cbt_cba"
        self.cursor.execute(query_consulta) #Ejecutamos la consulta
        tamanio_bdd = self.cursor.fetchone()[0] #Obtenemos el tamaño de la bdd
        
        #Obtenemos la cantidad de datos del dataframe construido
        tamanio_df = len(df)
        
        print(tamanio_bdd,tamanio_df)

        return tamanio_df,tamanio_bdd
    
    #Objetivo: almacenar en la tabla cbt_cba con los datos nuevos
    def cargar_tabla_datalake(self,df_cargar):

        query_insertar_datos = "INSERT INTO cbt_cba VALUES (%s, %s, %s, %s, %s,%s, %s)"

        for index,row in df_cargar.iterrows():
            
            #Obtenemos los valores de cada fila del DF
            values = (row['Fecha'],row['CBA_Adulto'],row['CBT_Adulto'],row['CBA_Hogar'],row['CBT_Hogar'],row['cba_nea'],row['cbt_nea'])


            # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
            values = [None if pd.isna(v) else v for v in values]

            #Realizamos carga
            self.cursor.execute(query_insertar_datos,values)

        #Cerramos conexiones
        self.close_conections()


# =========================================================================================== #        
# =========================================================================================== #
        

# =========================================================================================== #
                # ==== SECCION CORRESPONDIENTE AL DATAWAREHOUSE ==== #
# =========================================================================================== #

    """
    Tabla a1: Contiene los datos que corresponden al correo de CBA y CBT.
    Datos:
        - Fecha
        - CBA de GBA
        - CBT de GBA
        - CBA de NEA 
        - CBT de NEA
        - CBA FAMILIAR EN EL NEA
        - CBT FAMILIAR EN EL NEA
        - Var. Mensual, Interanual, Y acumulado de CBA y CBT del NEA.
        - Var. mensual e Interanual de IPC.

    """
    def table_a1(self):

        #Conectamos la BDD para empezar a hacer los calculos,
        self.connect_db()
        df = self.create_date_a1()
        self.load_date_mail(df)



    #Objetivo: calcular los valores necesarios para tabla A1. Los datos de IPC lo tratamos en otra funcion
    def create_date_a1(self):

        #Extraemos datalake
        select_query = "SELECT * FROM cbt_cba"
        df_bdd = pd.read_sql(select_query,self.conn)

        
        df = pd.DataFrame()


        #Asignacion de CBA y CBT de nacion y del NEA. Tambien fecha
        df['fecha'] = df_bdd['fecha']
        df['cba_gba'] = df_bdd['cba_adulto']
        df['cbt_gna'] =  df_bdd['cbt_adulto']
        df['cba_nea'] = df_bdd['cba_nea']
        df['cbt_nea'] = df_bdd['cbt_nea']

        #Asignacion de los valores familiares en el nea. Valor de ponderacion: 3.09
        df['cba_nea_familia'] = df['cba_nea'] * 3.09
        df['cbt_nea_familia'] = df['cbt_nea'] * 3.09


        #==== CALCULOS DE LA CANASTA BASICA ALIMENTARIA

        #=== Creacion de variaciones mensual, interanual PARA CBA
        df['vmensual_cba'] = ((df['cba_nea'] / df['cba_nea'].shift(1)) - 1) * 100  #--> Var. Mensual de cba NEA
        df['vinter_cba'] = ((df['cba_nea'] / df['cba_nea'].shift(12)) - 1) * 100 #--> Var. Interanual de cba NEA


        # === Creacion de las variaciones acumuladas
        #Para el acumulado necesitamos detectar diciembre, vamos a usar la fecha maxima para esto

        df['fecha'] = pd.to_datetime(df['fecha'])#--> Transformamos los datos para maniobrarlos

        #Tomamos los años para recorrerlos
        anios = list(set(df['fecha'].dt.year))

        #Lista de variaciones acumuladas de canasta basica alimentaria
        var_acumuladas_cba = list()

        for anio in anios:

            valores_anio = list(df['cba_nea'][df['fecha'].dt.year == anio].values)

            #Rescatamos valor diciembre del año anterior - Si falla quiere decir que no tenemos ese dato
            try:
                val_diciembre_año_anterior = df['cba_nea'][ (df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ].values[0] #--> Obtencion del valor puro de cba NEA
                #Calculamos variaciones acumuladas por cada año valido
                for valor in valores_anio:

                    var_acumulada = ((valor / val_diciembre_año_anterior) - 1) * 100
                    var_acumuladas_cba.append(var_acumulada)

            except: #No se encontro el valor de diciembre, por ende no se calculara estimaciones para ese periodo. Se asignan valores nulos
                for valor_error in valores_anio:
                    var_acumuladas_cba.append(None)

        
        #Asignaciones de variaciones acumuladas de la canasta basica alimentaria      
        df['vacum_cba'] = var_acumuladas_cba

        #==== CALCULOS DE LA CANASTA BASICA TOTAL
        
        #=== Creacion de variaciones mensual, interanual PARA CBT
        df['vmensual_cbt'] = ((df['cbt_nea'] / df['cbt_nea'].shift(1)) - 1) * 100  #--> Var. Mensual de cbt NEA
        df['vinter_cbt'] = ((df['cbt_nea'] / df['cbt_nea'].shift(12)) - 1) * 100 #--> Var. Interanual de cbt NEA

        #Lista de variaciones acumuladas de canasta basica alimentaria
        var_acumuladas_cbt = list()

        for anio in anios:

            valores_anio = list(df['cbt_nea'][df['fecha'].dt.year == anio].values)

            #Rescatamos valor diciembre del año anterior - Si falla quiere decir que no tenemos ese dato
            try:
                val_diciembre_año_anterior = df['cbt_nea'][ (df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ].values[0] #--> Obtencion del valor puro de cbt NEA

                #Calculamos variaciones acumuladas por cada año valido
                for valor in valores_anio:

                    var_acumulada = ((valor / val_diciembre_año_anterior) - 1) * 100
                    var_acumuladas_cbt.append(var_acumulada)

            except: #No se encontro el valor de diciembre, por ende no se calculara estimaciones para ese periodo. Se asignan valores nulos
                for valor_error in valores_anio:
                    var_acumuladas_cbt.append(None)

        #Asignaciones de variaciones acumuladas de la canasta basica total
        df['vacum_cbt'] = var_acumuladas_cbt

        

        #==== INCORPORACION DE IPC

        #Para añadir IPC es necesario cerrar las conexiones con la base de datos de sociodemografico y abrirlas con la de economico
        self.close_conections()#--> Cerramos
        self.set_database("datalake_economico") #--> Cambiamos BDD
        self.connect_db() #--> Reconectarnos al datalake economico
        
        #Calculamos las variaciones necesarias del IPC
        self.connecting_with_ipc(df)

        #Cerramos las conexiones nuevamente
        self.close_conections()
        

        return df


    #En esta funcion realizamos los calculos sobre las variaciones de IPC
    def connecting_with_ipc(self,df):

        #Explicacion: la consulta ira a ipc_valores, y traera todos los datos de la region 5 (NEA) y de la categoria 1 (Nivel general del IPC)
        query_consulta = f'SELECT * FROM ipc_valores WHERE ID_Region = 5 and ID_Categoria = 1'

        #Construccion de dataframe a partir de la consulta
        df_bdd = pd.read_sql(query_consulta,self.conn)

        print(df_bdd)        
        # ==== CALCULOS DE VARIACIONES MENSUALES Y INTERANUALES

        #=== Creamos columnas para evitar conflictos de manipulacion del datafarame
        df['vmensual_nea_ipc'] = float('nan')
        df['vinter_nea_ipc'] = float('nan')

        #=== Tomamos la primera fecha del grupo de IPC -- Esto porque CBT y CBA empieza su historico antes que los datos oficiales de IPC
        firt_date =pd.to_datetime (df_bdd['fecha'].values[0])

        #=== Creacion de variaciones mensual, interanual PARA IPC del NEA
        df['vmensual_nea_ipc'].iloc[df['fecha'] >= firt_date] =( df_bdd['valor'] / df_bdd['valor'].shift(1) - 1) * 100  #--> Var. Mensual de IPC
        df['vinter_nea_ipc'].iloc[df['fecha'] >= firt_date] = ((df_bdd['valor'] / df_bdd['valor'].shift(12)) - 1) * 100 #--> Var. Interanual de IPC


    #Objetivo: cargar los datos correspondientes al correo de CBT y CBA. Es llamado en la funcion table_a1()
    def load_date_mail(self,df):
        
        """
        Para la carga es necesario:
        1 - Cambiar por setter el nombre de la bdd
        2 - Conectarnos a la bdd
        3 - Truncar la tabla correspondiente (ya que trabajamos con estimaciones)
        4 - Cargar los datos nuevos
        """

        #Paso 1 - vamos a usar DWH de socio
        self.set_database("dwh_sociodemografico")

        #Paso 2- Conectamos a la bdd
        self.connect_db()

        #Paso 3 - Truncamos la tabla usando la query y el conector. Ejecutamos la consulta
        query_delete_table = "TRUNCATE correo_cbt_cba"
        self.cursor.execute(query_delete_table)


        #4 - Cargamos los datos usando una query y el conector. Ejecutamos las consultas. PARA ESTE PASO ES OBLIGATORIO TRABAJAR CON SQLAlchemy
        engine = create_engine(f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{3306}/{self.database}")
        df.to_sql(name="correo_cbt_cba", con=engine, if_exists='replace', index=False)
        

        print("======")
        print("Los datos correspondiente a los corrreos de CBT y CBA han sido actualizados.")
        print("======")


# =========================================================================================== #        
# =========================================================================================== #
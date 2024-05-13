import pymysql
import pandas as pd
from sqlalchemy import create_engine
from numpy import  trunc


class conexionBaseDatos:

    #Inicializacion de variables en la clase
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.cursor = None
        self.conn = None


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LOS SETTERs ==== #
    # =========================================================================================== #  

    #Objetivo: cambiar el nombre de la base de datos para reconectarnos a otra.
    def set_database(self,new_name):

        self.database = new_name


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #        

    #Objetivo: conectar a la base de datos
    def connect_db(self):

            self.conn = pymysql.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()


    #Objetivo: guardar los ultimos cambios hechos y cerrar las conexiones
    def close_connections(self):
        self.conn.commit()
        self.conn.close()
        self.cursor.close()


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE AL DATALAKE ==== #
    # =========================================================================================== #      
        

    #Objetivo: cargar los datos de SIPA al datalake_economico
    def load_datalake(self,df):

        #Se establece la conexion a la BDD
        self.connect_db()

        len_bdd,len_df = self.check_lens(df)

        if len_df > len_bdd:

            #Primero truncamos por ser datos estimativos
            query_delete = 'TRUNCATE sipa_valores'
            self.cursor.execute(query_delete)

            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df.to_sql(name="sipa_valores", con=engine, if_exists='append', index=False)

            #Guardamos cambios 
            self.conn.commit()

            #=== ZONA DE CARGA DEL DATAWAREHOUSE
            self.table_analytics_sipa()
            self.table_analytics_sipa_nea()

            #Se retorna true para enviar el correo
            return True
        else:
            print("\n - No existen datos nuevos de SIPA para cargar \n")
            return False


    #Objetivo: obtener los datos del dataframe, y de la tabla almacenada en la bdd
    def check_lens(self,df):
        
        # Verificar cuantas filas tiene la tabla de mysql ejecutando la consulta
        select_query = "SELECT COUNT(*) FROM sipa_valores"
        self.cursor.execute(select_query)
        #Tamaño de la tabla de la BDD
        len_bdd = self.cursor.fetchone()[0]

        #Tamaño del dataframe
        len_df = len(df)

        return len_bdd,len_df


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LA TABLA DE DATOS NACIONALES (DWH) ==== #
    # =========================================================================================== #           

    """
    Esta tabla contiene los datos correspondiente al correo de SIPA
    Datos:
        - Empleo privado registrado
        - Empleo publico registrado
        - Monotributista independientes
        - Monotributistas sociales
        - Empleo en casas particulares registrado
        - Trabajadores independientes autonomos
        - Trabajo registrado a nivel nacional
        - Variacion mensual del trabajo registrado
        - Variacion interanual del trabajo registrado
        - Variacion acumulada del trabajo registrado
        - Total empleo en Corrientes - Acompañado de var. mensual, interanual y acumulado. Tambien porcentaje representativo en el nea. Diferencias de cada variacion.
        - Total empleo en misiones - Acompañado de var. mensual, interanual y acumulado. Tambien porcentaje representativo en el nea. Diferencias de cada variacion.
        - Total empleo en chaco - Acompañado de var. mensual, interanual y acumulado. Tambien porcentaje representativo en el nea. Diferencias de cada variacion.
        - total empleo en formosa - Acompañado de var. mensual, interanual y acumulado. Tambien porcentaje representativo en el nea. Diferencias de cada variacion.
        - Total de empleo en el NEA - Acompañado de var. mensual, interanual y acumulado. Tambien porcentaje representativo en NACION. Diferencias de cada variacion.
        - Total de empleo en nacion - - Acompañado de var. mensual, interanual y acumulado.  Diferencias de cada variacion.    
    """
    def table_analytics_sipa(self):

        #Df que contendra los datos a cargar
        df = pd.DataFrame()
        self.get_percentages(df)
        self.get_variances_nation(df)

        df['fecha'] = pd.to_datetime(df['fecha']).dt.date

        print(df)
        
        #Carga de tabla
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/dwh_economico")
        df.to_sql(name="empleo_nacional_porcentajes_variaciones", con=engine,  if_exists='replace', index=True)

        #Guardamos cambios
        self.conn.commit()


    #Objetivo: obtener los porcentajes representativos de cada tipo de empleo. Esto es solo aplicable a los datos de nacion.
    #Los datos de cada provincia estan representados con el tipo de registro 
    def get_percentages(self,df):

        #Con la consulta extraemos los datos del sipa
        select_query = "SELECT * FROM sipa_valores WHERE id_provincia = 1"
        df_bdd = pd.read_sql(select_query,self.conn)       

        #ASIGNACION DE FECHAS
        df['fecha'] = list(df_bdd['fecha'][df_bdd['id_tipo_registro'] == 8])

        #=== Asignacion de totales por cada tipo de empleo - No se incluye tipo 1 (solo empleo - esto es mas para provincias individuales)

        #Asignacion del empleo total, se toman los dato y luego se aplica la funcion para que tomemos solo 3 decimales
        df['empleo_total'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 8])
        df['empleo_total'] = df['empleo_total'].apply(lambda x: trunc(x * 1000) / 1000)

        #Asignacion de los demas totales
        df['empleo_privado'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 2])
        df['empleo_publico'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 3])
        df['empleo_casas_particulares'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 4])
        df['empleo_independiente_autonomo'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 5])
        df['empleo_independiente_monotributo'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 6])
        df['empleo_monotributo_social'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_tipo_registro'] == 7])

        #Calculos de niveles porcentuales en base al total
        df['p_empleo_privado'] = (df['empleo_privado'] * 100) / df['empleo_total']
        df['p_empleo_publico']= (df['empleo_publico'] * 100) / df['empleo_total']
        df['p_empleo_casas_particulares'] = (df['empleo_casas_particulares'] * 100) / df['empleo_total']
        df['p_empleo_independiente_autonomo'] = (df['empleo_independiente_autonomo'] * 100) / df['empleo_total']
        df['p_empleo_independiente_monotributo'] = (df['empleo_independiente_monotributo'] * 100) / df['empleo_total']
        df['p_empleo_monotributo_social'] = (df['empleo_monotributo_social'] * 100) / df['empleo_total']

        print("PORCENTAJES DEL DF")
        print(df)

    #Objetivo: calcular las variaciones mensuales, interanuales y acumuladas a nivel nacional del total de los empleos     
    def get_variances_nation(self,df):

        #=== Creacion de variaciones mensual, interanual del TOTAL DE EMPLEO A NIVEL NACIONAL, y del EMPLEO PRIVADO
        df['vmensual_empleo_total'] = ((df['empleo_total'] / df['empleo_total'].shift(1)) - 1) * 100  #--> Var. Mensual de empleo nacion
        df['vinter_empleo_total'] = ((df['empleo_total'] / df['empleo_total'].shift(12)) - 1) * 100 #--> Var. Interanual de empleo nacion

        df['vmensual_empleo_privado'] = ((df['empleo_privado'] / df['empleo_privado'].shift(1)) - 1) * 100  #--> Var. Mensual de Empleo Privado
        df['vinter_empleo_privado'] = ((df['empleo_privado'] / df['empleo_privado'].shift(12)) - 1) * 100 #--> Var. Interanual de Empleo privado

        #=== Calculo de variaciones acumuladas

        #Transformamos tipo fecha para maniobrarla
        df['fecha'] = pd.to_datetime(df['fecha'])

        #Tomamos los años para recorrerlos
        anios = sorted(list(set(df['fecha'].dt.year)))

        #Creamos columnas para que no existan problemas de compatibilidad
        df['vacum_empleo_total'] = float('nan')
        df['vacum_empleo_privado'] = float('nan')
 
        for anio in anios:

            val_diciembre = df[['empleo_total','empleo_privado']][(df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ] #--> Obtencion del valor puro de cba NEA

            diciembre_total_empleo= val_diciembre['empleo_total']
            diciembre_empleo_privado = val_diciembre['empleo_privado']

            #Calculamos variaciones acumuladas por cada año valido
            try:
                df.loc[df['fecha'].dt.year == anio,'vacum_empleo_total'] = ((df['empleo_total'][df['fecha'].dt.year == anio] / diciembre_total_empleo.values[0]) - 1) * 100     
                df.loc[df['fecha'].dt.year == anio,'vacum_empleo_privado'] = ((df['empleo_privado'][df['fecha'].dt.year == anio] / diciembre_empleo_privado.values[0]) - 1) * 100            
       
            except:
                pass
        
        #Asignaciones de variaciones acumuladas del total de empleo en nacion
        #df['vacum_empleo_total'] = var_acumuladas

    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LA TABLA DE DATOS DEL NEA Y SUS PROVINCIAS (DWH) ==== #
    # =========================================================================================== #    
        

    def table_analytics_sipa_nea(self):

        df = pd.DataFrame()
        self.get_variances_nea(df)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
    
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/dwh_economico")
        df.to_sql(name="empleo_nea_variaciones", con=engine, if_exists='replace', index=True)

        #Guardamos cambios del NEA
        self.conn.commit()

    def get_variances_nea(self, df):

        #Buscamos los datos con la query de cada provincia del NEA (Corrientes = 18, Misiones=54, Chaco = 22, Formosa = 34)
        select_query = "SELECT fecha, id_provincia, cantidad_con_estacionalidad FROM sipa_valores WHERE id_provincia = 18 OR id_provincia = 22 OR id_provincia = 54 OR id_provincia = 34"
        df_bdd = pd.read_sql(select_query,self.conn)

        #--> Transformamos los datos de fecha para maniobrarlos
        df['fecha'] = sorted(set(pd.to_datetime(df_bdd['fecha'])))

        #Asignacion de totales
        df['total_corrientes'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == 18] * 1000)
        df['total_misiones'] = list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == 54]* 1000)
        df['total_chaco']= list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == 22]* 1000)
        df['total_formosa']= list(df_bdd['cantidad_con_estacionalidad'][df_bdd['id_provincia'] == 34]* 1000)
        df['total_nea'] = df['total_corrientes'] + df['total_misiones'] + df['total_chaco'] + df['total_formosa']

        #Asignacion de variaciones mensuales
        df['vmensual_nea'] = ( df['total_nea'] / df['total_nea'].shift(1) - 1) * 100
        df['vmensual_corrientes'] =( df['total_corrientes'] / df['total_corrientes'].shift(1) - 1) * 100
        df['vmensual_misiones'] = ( df['total_misiones'] / df['total_misiones'].shift(1) - 1) * 100
        df['vmensual_chaco'] = ( df['total_chaco'] / df['total_chaco'].shift(1) - 1) * 100
        df['vmensual_formosa'] = ( df['total_formosa'] / df['total_formosa'].shift(1) - 1) * 100

        #Asignacion de variaciones interanuales
        df['vinter_nea'] = ( df['total_nea'] / df['total_nea'].shift(12) - 1) * 100
        df['vinter_corrientes'] = ( df['total_corrientes'] / df['total_corrientes'].shift(12) - 1) * 100
        df['vinter_misiones'] = ( df['total_misiones'] / df['total_misiones'].shift(12) - 1) * 100
        df['vinter_chaco'] = ( df['total_chaco'] / df['total_chaco'].shift(12) - 1) * 100
        df['vinter_formosa'] = ( df['total_formosa'] / df['total_formosa'].shift(12) - 1) * 100


        #=== Asignacion de variaciones acumuladas
        df['vacum_nea'] = float('nan')
        df['vacum_corrientes'] = float('nan')
        df['vacum_misiones'] = float('nan')
        df['vacum_chaco'] = float('nan')
        df['vacum_formosa'] = float('nan')


        print(df['fecha'].dt.year)
        #Tomamos los años para recorrerlos
        anios = sorted(list(set(df['fecha'].dt.year)))

        for anio in anios:
        
            
            val_diciembre = df[['total_corrientes','total_misiones','total_chaco','total_formosa','total_nea']][(df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ] #--> Obtencion del valor puro de cba NEA

            diciembre_corrientes = val_diciembre['total_corrientes']
            diciembre_misiones = val_diciembre['total_misiones']
            diciembre_chaco = val_diciembre['total_chaco']
            diciembre_formosa = val_diciembre['total_formosa']
            diciembre_nea = val_diciembre['total_nea']

            #Calculamos variaciones acumuladas por cada año valido
            try:
                df.loc[df['fecha'].dt.year == anio,'vacum_corrientes'] = ((df['total_corrientes'][df['fecha'].dt.year == anio] / diciembre_corrientes.values[0]) - 1) * 100
                df.loc[df['fecha'].dt.year == anio,'vacum_misiones'] = ((df['total_misiones'][df['fecha'].dt.year == anio] / diciembre_misiones.values[0]) - 1) * 100
                df.loc[df['fecha'].dt.year == anio,'vacum_chaco'] = ((df['total_chaco'][df['fecha'].dt.year == anio] / diciembre_chaco.values[0]) - 1) * 100
                df.loc[df['fecha'].dt.year == anio,'vacum_formosa'] = ((df['total_formosa'][df['fecha'].dt.year == anio] / diciembre_formosa.values[0]) - 1) * 100
                df.loc[df['fecha'].dt.year == anio,'vacum_nea'] = ((df['total_nea'][df['fecha'].dt.year == anio] / diciembre_nea.values[0]) - 1) * 100


            except:
                pass



    # =========================================================================================== #    
    # =========================================================================================== #    
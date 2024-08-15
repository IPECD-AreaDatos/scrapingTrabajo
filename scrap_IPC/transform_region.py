"""
Archivo destinado a construir el DATAFRAME  que contendra:
* Valores de indice, Vars. mensuales. y Vars. interanuales. Cada dato dividido por REGION,CATEGORIA,DIVISION,SUBDIVISION
"""

import pandas as pd
from sqlalchemy import create_engine
import os
from unidecode import unidecode
from numpy import unique, nan

class TransformRegiones:

    #DEfinicion de atributos
    def __init__(self, host, user, password, database):

        #Conectamos a la BDD
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/{database}")

        #Diccionario con el que realizaremos codificacion
        self.diccionario = None


    #Objetivo: Construir el diccionario con el que que realizaremos el mapeo a nivel de SUBDIVISION.
    def construir_diccionario(self):

        #Obtenemos tabla de subdivisiones
        tabla_subdivision = pd.read_sql_query("SELECT * FROM ipc_subdivision", con=self.engine) 

        # Crear una columna 'codigo' combinando las columnas 'categoria', 'division' y 'subdivision' creando una lista con 3 valores enteros
        tabla_subdivision['codigo'] = tabla_subdivision.apply(lambda row: [int(row['id_categoria']), int(row['id_division']), int(row['id_subdivision'])],axis=1)

        # Crear un diccionario de mapeo entre 'nombre' y la lista de 'codigo' que le corresponde
        self.diccionario = dict(zip(tabla_subdivision['nombre'], tabla_subdivision['codigo']))

        #Formateamos las keys, a un formato sin acentos, mayusculas, espacios, tildes
        self.diccionario  = {self.formatear_key(key): value for key, value in self.diccionario.items()}


    #Objetivo: formatear las keys del diciconario generado || Tambien se usa para formatear las subdivisiones que se presentan en el excel
    def formatear_key(self,key):
        # Convertir a minúsculas
        key = key.lower()
        # Eliminar tildes y acentos
        key = unidecode(key)
        # Eliminar comas, puntos y espacios
        key = key.replace(',', '').replace('.', '').replace(' ', '')
        return key


    #Objetivo: construir la ruta de acceso a la carpeta FILES
    def construccion_rutas(self):

        #Carpeta scrap_IPC
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')

        #Direcciones de los Excels
        path_ipc_apertura = os.path.join(ruta_carpeta_files, 'sh_ipc_aperturas.xls') #Dato por region
        path_ipc_mes_ano = os.path.join(ruta_carpeta_files, 'sh_ipc_mes_ano.xls') #Dato nacional

        return path_ipc_apertura, path_ipc_mes_ano
    

    #Objetivo: construir el DF de los datos nacionales del IPC, usando el archivo 'path_ipc_mes_ano'.
    def construir_datos_nacionales(self,path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor):

        #==== OBTENCION DE LOS DATOS

        #Obtenemos los datos del excel
        df_nacion = pd.read_excel(path_ipc_mes_ano, sheet_name=num_hoja_nacion, skiprows=5, nrows=16)

        #Realizamos un corte del dataframe, eliminando las primeras filas que son filas en blanco
        df_nacion = df_nacion.iloc[3:]

        #==== MANIPULACION DE LOS DATOS

        """
        Se resume en:
            - Codificar datos con diccionario, obteniendo una lista
            - La lista construida la usamos para generar columnas separadas, cada col. representa un elemento de la lista.
            - Hacemos cosas adicionales como, ordenar columnas y anadir la region
        """

        #Cambiamos el nombre de la primera columna, de Total nacional a claves_listas
        df_nacion.rename(columns={'Total nacional': 'claves_listas'}, inplace=True)


        #==== FORMATEO DE DICCIONARIO

        """
        Funcionamiento: Al realizar apply, se aplica la funcion 'formatear_key' a cada valor de la serie.
        No es necesario pasarle parametros, ya que la funcion ya apply, ya entiende que se aplica a las
        columnas indicadas.
        """
        df_nacion['claves_listas'] = df_nacion['claves_listas'].apply(self.formatear_key)



        #Realizamos el mapeo de los datos
        df_nacion['claves_listas'] = df_nacion['claves_listas'].map(self.diccionario)

        #La lista generada en 'claves_listas', que contiene 3 valores, la usaremos para generar 3 columnas adicionales.
        #Cada columna tiene un valor que representa una CATEGORIA | DIVISION | SUBDIVISION
        df_nacion[['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df_nacion['claves_listas'].tolist(), index=df_nacion.index)
        df_nacion = df_nacion.drop('claves_listas',axis=1)

        #Trasponemos usando las CATEGORIA | DIVISION | SUBDIVISION y el nombre de la col de los valores, sera el pasado por parametro 
        df_nacion_melted = pd.melt(df_nacion, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name=nombre_col_valor)

        #=== PASOS FINALES

        #Añadimos la region
        df_nacion_melted['id_region'] = 1

        #Ordenamos la tabla, para que coincinda con la que tenemos en la bdd
        columnas_ordenadas = ['fecha', 'id_region', 'id_categoria', 'id_division', 'id_subdivision', nombre_col_valor]

        # Reorganizamos las columnas del DataFrame
        df_nacion_melted = df_nacion_melted[columnas_ordenadas]

        return df_nacion_melted



    def armado_dfs(self,path_ipc_apertura,num_hoja_nacion,filas_a_eliminar,nombre_col_valor):
        
        #=== LECUTURA DEL EXCEL DIVIDO POR REGION
        df_original = pd.read_excel(path_ipc_apertura, sheet_name=num_hoja_nacion, skiprows=4, nrows=295)


        # == Obtencion de los indices

        # Obtenemos los indice donde esta el titulo de cada region, donde tiene el valor " Region 'ZONA' "
        indice_gba = df_original[df_original.iloc[:, 0] == 'Región GBA'].index[0]
        indice_pampeana = df_original[df_original.iloc[:, 0] == 'Región Pampeana'].index[0]
        indice_noroeste = df_original[df_original.iloc[:, 0] == 'Región Noroeste'].index[0]
        indice_noreste = df_original[df_original.iloc[:, 0] == 'Región Noreste'].index[0] 
        indice_cuyo = df_original[df_original.iloc[:, 0] == 'Región Cuyo'].index[0] 
        indice_patagonia = df_original[df_original.iloc[:, 0] == 'Región Patagonia'].index[0] 

        """
        Usando los indices, iremos obteniendo "CORTES" del df_original, cada "CORTE" representa los datos de una region.

        Detalles: 
        
        GBA tiene 44 subdivisiones a codificar, mientras que el resto de regiones tiene 42.
        Para alcanzar las 44 subdivisiones de gba, al indice encontrado le sumamos 48 filas de lectura.
        Para el resto, solo sumamos 46 para alcanzar todas las subdivisiones.
        
        """

        #Creacion de los dataframes
        df_gba = df_original.iloc[indice_gba : indice_gba + 48] 
        df_pampeana = df_original.iloc[indice_pampeana : indice_pampeana + 46]
        df_noroeste = df_original.iloc[indice_noroeste : indice_noroeste + 46]
        df_noreste = df_original.iloc[indice_noreste : indice_noreste + 46]
        df_cuyo = df_original.iloc[indice_cuyo : indice_cuyo + 46]
        df_patagonia = df_original.iloc[indice_patagonia : indice_patagonia + 46]


        #Crearemos dos listas, una de los DFS generados, y otra con la region de los DFS que la 
        #usaremos para asignar la region a cada DF.
        lista_region = [2,3,4,5,6,7]
        lista_dfs = [df_gba,df_pampeana,df_noroeste,df_noreste,df_cuyo,df_patagonia]

        #Acumulador de datos ordenamos
        df_acum = pd.DataFrame(columns=['fecha','id_region','id_categoria','id_division','id_subdivision',nombre_col_valor])
        

        """
        Recorremos los Dataframes, lo que se realiza es:

        1 - Asignamos la primera fila como columnas.
        2 - Eliminamos las N (Definido por el parametro pasado) primeras filas, todos los dfs tienen las primeras filas sin valores.
        3 - Codificacion de las subdivisiones usando el diccionario, obteniendo en su lugar una lista de valores.
        4 - Usamos las listas de paso 3, y por cada valor creamos una columna, en el orden CATEGORIA | DIVISION | SUBDIVISION
        5 - Trasponemos las columnas teniendo en cuenta las CATEGORIA | DIVISION | SUBDIVISION asignadas en 4.
        6 - Asignamos la region y ordenamos los datos

        """

        
        for id_region, df_region in zip(lista_region,lista_dfs):

            #Generamos una copia por las dudas
            df = df_region.copy()

            #Paso 1
            # Eliminar filas donde todos los valores son NaN || se aplica porque al buscar los valores, la ultima fila contiene valores Nulos
            df.columns = df.iloc[0]
            df = df.iloc[1:,]

            #Paso 2, Eliminar aquellas filas que presenten TODAS las filas nulas
            df = df.dropna(how='all')
            
            # ===== EXCEPCION DE LA FUNCION QUE SOLO SE UTILIZA PARA OBTENER LOS VALORES ===== #

            #Esta es una excepcion que se usa para poder obtener los valores del IPC. Ya que todos sus DFs presentan una ultima fila en blanco.
            #Entonces la revisamos, y si realmente es nulo, la eliminamos
            #if (df.iloc[-1].isnull().any()):
            #    df = df.drop(df.index[-1])

            # ====== FIN DE LA EXCEPCION ====== #

            #Paso 3 - Accedemos a la primera columna con el supuesto de "no saber el nombre de la columna"         
            nom_col = df.columns[0]

            #print(df)
   
            #Formateamos los nombres de las subdivisiones, para que coincidan con el diccionario
            df[nom_col] = df[nom_col].apply(self.formatear_key)

            #Realizamos, efectivamente el mapeo
            df.iloc[:, 0] = df[nom_col].map(self.diccionario)

            #Paso 4
            df[['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df[nom_col].tolist(), index=df.index)

            #Con los datos generados, borraremos la primera columna - O sino nos rompe el formato deseado.
            df = df.drop(nom_col,axis=1)
                        
            #Paso 5
            df_melted = pd.melt(df, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name=nombre_col_valor)

            #Paso 6 
            df_melted['id_region'] = id_region
            columnas_ordenadas = ['fecha', 'id_region', 'id_categoria', 'id_division', 'id_subdivision',nombre_col_valor]
            df_melted = df_melted[columnas_ordenadas]


            #Iremos concatenando los dataframes
            df_acum = pd.concat([df_acum,df_melted])

        return df_acum
    

    #Objetivo: juntar todos los datos, logrando un dataset que contenga: valores, vars. mensuales, y vars. interanuales.
    def concatenacion_final(self,df_valores,df_variaciones,df_variaciones_interanuales):
        
        #Realizamos una copia para no afectar el df original
        df_final = df_valores.copy()
        df_final['fecha'] = pd.to_datetime(df_final['fecha'])
        df_variaciones['fecha'] = pd.to_datetime(df_variaciones['fecha'])
        df_variaciones_interanuales['fecha'] = pd.to_datetime(df_variaciones_interanuales['fecha'])

        # Fusionar los DataFrames en función de las columnas comunes 

        #Lo que se hace es revisar, que claves de df_final coinciden con las de df_variaciones. En caso de coincidir, se asigna el valor 'var_mensual'
        df_final = pd.merge(df_final, df_variaciones[['fecha', 'id_region', 'id_subdivision', 'var_mensual']], 
                        on=['fecha', 'id_region', 'id_subdivision'], how='left')
        
        #Lo que se hace es revisar, que claves de df_final coinciden con las de df_variaciones_interanuales. En caso de coincidir, se asigna el valor 'var_interanual'
        df_final = pd.merge(df_final, df_variaciones_interanuales[['fecha', 'id_region', 'id_subdivision', 'var_interanual']], 
                        on=['fecha', 'id_region', 'id_subdivision'], how='left')
        
        return df_final
    
    #Objetivo: calcular la var. acumulada por cada subdivision
    def calculo_var_acumulada(self,dfs_concatenados):

        #Creacion de lista de subdivisiones
        subdivisiones_unicas = list(range(1,46))
        regiones = [1,2,3,4,5,6,7]
        anios = unique(dfs_concatenados['fecha'].dt.year)

        #lista de variaciones acumuladas
        lista_acumuladas = []

        """
        Explicacion: Para calcular las vars. acumuladas nos basamos en el uso de las
        subdivisiones. La estrategia es:

        1 - Buscar los datos por REGION|SUBDIVISION|ANO
        2 - Previo al calculo de la var. acumulada necesitamos el dato.
            de la misma REGION|SUBDIVISION pero del ano anterior.
        3 - Si el dato existe, se busca y calcularemos la var. acumulada.
            En el caso de no exitir, asignaremos un valor nulo en su lugar.

        """
        for region in regiones:
            for subdivision in subdivisiones_unicas:
                for anio in anios:
                    
                    #Buscamos el dato de Dic. del ano anterior
                    try:
                        dato_anio_anterior = dfs_concatenados[['valor']][(dfs_concatenados['id_subdivision'] == subdivision) & (dfs_concatenados['id_region'] == region) & (dfs_concatenados['fecha'].dt.year == (anio - 1) ) &(dfs_concatenados['fecha'].dt.month == 12 )].iloc[-1]
                        dato_anio_anterior = dato_anio_anterior.values[0]

                    #Si el dato no existe, asignamos un Nulo a la variable
                    except:
                        dato_anio_anterior = nan


                    #Buscamos los datos del ANO buscado.
                    data = dfs_concatenados[
                        (dfs_concatenados['id_subdivision'] == subdivision) &
                        (dfs_concatenados['id_region'] == region) &
                        (dfs_concatenados['fecha'].dt.year == anio)
                        ]
                    
                    #Calculamos la var. acumulada para todos los datos obtenidos
                    variacion_acumulada = (data['valor'] / dato_anio_anterior) - 1

                    #Recorremos el bloque de datos generado, y lo anadimos a nuestra lista
                    for valor in variacion_acumulada:
                        lista_acumuladas.append(valor)


        #Asignacion final de los datos
        dfs_concatenados['var_acumulada'] = lista_acumuladas

                    
                    

    def main(self):

        #Construccion del diccionario
        self.construir_diccionario()

        #Construccion del rutas
        path_ipc_apertura, path_ipc_mes_ano = self.construccion_rutas()

        # ===== OBTENCION DE LOS VALORES DE INDICE TANTO NACIONALES COMO REGIONALES ==== #
     
        #ESTABLECEMOS LAS CONDICIONES INICIALES DE LOS VALORES
        num_hoja_nacion = 2
        num_hoja_regiones = 2
        filas_a_eliminar = 1
        nombre_col_valor = 'valor'

        #Obtencion de los valores nacionales
        df_nacion_valores = self.construir_datos_nacionales(path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor)


        #Obtencion de los valores por region
        df_region_valores = self.armado_dfs(path_ipc_apertura,num_hoja_regiones,filas_a_eliminar,nombre_col_valor)

        #Concatenamos 
        df_valores = pd.concat([df_nacion_valores,df_region_valores])

        #Ordenamos por region, y subdivision, sirve para futuro analisis. Tambien reseteamos el indice.
        df_valores = df_valores.sort_values(['id_region','id_subdivision'],ascending=True)
        df_valores = df_valores.reset_index(drop=True)


        # ===== OBTENCION DE LAS VARIACIONES NACIONALES Y REGIONALES ==== #

        #ESTABLECEMOS LAS CONDICIONES INICIALES DE LAS VARIACIONES
        num_hoja_nacion = 0
        num_hoja_regiones = 0
        filas_a_eliminar = 2
        nombre_col_valor = 'var_mensual'

        #Obtencion de las variaciones nacionales
        df_nacion_variaciones = self.construir_datos_nacionales(path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor)

        #Obtencion de las variaciones por region
        df_region_variaciones = self.armado_dfs(path_ipc_apertura,num_hoja_regiones,filas_a_eliminar,nombre_col_valor)

        #Concatenamos 
        df_variaciones = pd.concat([df_nacion_variaciones,df_region_variaciones])

        #Ordenamos por region, y subdivision, sirve para futuro analisis. Tambien reseteamos el indice.
        df_variaciones = df_variaciones.sort_values(['id_region','id_subdivision'],ascending=True)
        df_variaciones = df_variaciones.reset_index(drop=True)

        # ===== OBTENCION DE LAS VARIACIONES INTERANUALES NACIONALES Y REGIONALES ==== #        
    
        #ESTABLECEMOS LAS CONDICIONES INICIALES
        num_hoja_nacion = 1
        num_hoja_regiones = 1
        filas_a_eliminar = 1
        nombre_col_valor = 'var_interanual'
        
        #Obtencion de las variaciones interanuales nacionales
        df_nacion_variaciones_interanuales = self.construir_datos_nacionales(path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor)

        #Obtencion de las variaciones interanuales por region
        df_region_variaciones_interanuales = self.armado_dfs(path_ipc_apertura,num_hoja_regiones,filas_a_eliminar,nombre_col_valor)

        #Concatenamos resultado final
        df_variaciones_interanuales = pd.concat([df_nacion_variaciones_interanuales,df_region_variaciones_interanuales])

        #Ordenamos por region, y subdivision, sirve para futuro analisis. Tambien reseteamos el indice.
        df_variaciones_interanuales = df_variaciones_interanuales.sort_values(['id_region','id_subdivision'],ascending=True)
        df_variaciones_interanuales = df_variaciones_interanuales.reset_index(drop=True)

        #Concatenamos al df_valores los datos de las vars. mensuales y interanuales
        dfs_concatenados = self.concatenacion_final(df_valores,df_variaciones,df_variaciones_interanuales)

        #El dataset presenta caracteres '///', lo remplazamos por None.
        dfs_concatenados[['valor','var_mensual','var_interanual']] = dfs_concatenados[['valor','var_mensual','var_interanual']].replace('///', None).astype(float)

        #Transformaciones de dato porcentual a decimal.
        dfs_concatenados['var_mensual'] = dfs_concatenados['var_mensual'] / 100
        dfs_concatenados['var_interanual'] = dfs_concatenados['var_interanual'] / 100


        #Calculamos la var. acumulada y lo añadimos al dataset
        self.calculo_var_acumulada(dfs_concatenados)

        return dfs_concatenados




        
"""
Archivo destinado a construir el DATAFRAME  que contendra:

* Vars. mensuales. Cada dato dividido por REGION,CATEGORIA,DIVISION,SUBDIVISION
"""

import pandas as pd
from sqlalchemy import create_engine
import os
import sys

class TransformRegionesVariaciones:

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


            # Eliminar filas donde todos los valores son NaN || se aplica porque al buscar los valores, la ultima fila contiene valores Nulos

            #Paso 1
            df.columns = df.iloc[0]

            #Paso 2 (para eliminar las N primeras filas, tomaremos datos desde la N + 1 fila hasta el final)
            df = df.iloc[filas_a_eliminar + 1:]

            #Paso 3 - Accedemos a la primera columna con el supuesto de "no saber el nombre de la columna"         
            nom_col = df.columns[0]
            df.iloc[:, 0] = df[nom_col].map(self.diccionario)

            
            # ===== EXCEPCION DE LA FUNCION QUE SOLO SE UTILIZA PARA OBTENER LOS VALORES ===== #

            #Esta es una excepcion que se usa para poder obtener los valores del IPC. Ya que todos sus DFs presentan una ultima fila en blanco.
            #Entonces la revisamos, y si realmente es nulo, la eliminamos
            if (df.iloc[-1].isnull().any()):
                df = df.drop(df.index[-1])


            # ====== FIN DE LA EXCEPCION ====== #


            print(df)

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
 

    def main(self):

        #Construccion del diccionario
        self.construir_diccionario()

        #Construccion del rutas
        path_ipc_apertura, path_ipc_mes_ano = self.construccion_rutas()
        
     
        #ESTABLECEMOS LAS CONDICIONES INICIALES DE LOS VALORES
        num_hoja_nacion = 2
        num_hoja_regiones = 2
        filas_a_eliminar = 1
        nombre_col_valor = 'valor'

        df_nacion_valores = self.construir_datos_nacionales(path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor)


        df_region_valores = self.armado_dfs(path_ipc_apertura,num_hoja_regiones,filas_a_eliminar,nombre_col_valor)


        df_valores = pd.concat([df_nacion_valores,df_region_valores])




        return None
        #ESTABLECEMOS LAS CONDICIONES INICIALES DE LAS VARIACIONES
        num_hoja_nacion = 0
        num_hoja_regiones = 0
        filas_a_eliminar = 2
        nombre_col_valor = 'var_mensual'

        #Construccion del DF con datos nacionales
        df_nacion_variaciones = self.construir_datos_nacionales(path_ipc_mes_ano,num_hoja_nacion,nombre_col_valor)

        #Construccion del DF del resto de las regiones
        df_region_variaciones = self.armado_dfs(path_ipc_apertura,num_hoja_regiones,filas_a_eliminar,nombre_col_valor)

        #Concatenamos el resultado final
        df_variaciones = pd.concat([df_nacion_variaciones,df_region_variaciones])







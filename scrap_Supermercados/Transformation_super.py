#Objetivo: Obtener los datos del excel descargado, y brindarle el formato adecuado
import os
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta


class Transformation_Data:


    def contruccion_df(self):

        path_archivo = self.direccion_archivo()
        """
        Vamos a rescatar una columna, no importa cual, y vamos a obtener el tamaño de la primera seccion
        Luego vamos a iterrar cada seccion teniendo en cuenta este tamaño. 

        El dataframe 'df_aux' se descarta luego de usarlo
        El tamaño se almacena en la variable 'tamaño_secciones'
        """

        nombre_columna = ['fecha']

        df_aux = pd.read_excel(path_archivo,sheet_name=5,skiprows=5, usecols='C',names=nombre_columna)

        tamaño_secciones = self.construccion_lista_meses(df_aux['fecha']) #--> Obtenemos el tamaño que es la misma para cada pronvicia

        nombre_columnas = ['bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
                          'articulo_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']
        
        self.construccion_datframes(tamaño_secciones,path_archivo,nombre_columna)

    #Construimos una lista con todos los meses de la primera seccion
    def construccion_lista_meses(self,lista_fechas):


        lista_de_cadenas = [str(elemento) for elemento in lista_fechas]

        lista_retorno = []

        for valor in lista_de_cadenas:

            if valor == 'nan':
                break
            else:
                
                lista_retorno.append(valor)

        return len(lista_retorno)


    def direccion_archivo(self):

        # Direccion actual + nombre archivo = direccion del archivo
        directorio_actual  = os.path.dirname(os.path.abspath(__file__))
        nombre_archivo = '\\files\\encuesta_supermercado.xls'
        path_archivo = directorio_actual + nombre_archivo
        return path_archivo

    def concatenar_dataframes(self,tamaño_secciones,path_archivo):
        
        var_aux = 7
        nombre_columnas = ['bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
                            'articulo_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']
        
        print("=========================")
        df = pd.read_excel(path_archivo,sheet_name=5,skiprows= tamaño_secciones + 7 , usecols='e,f,g,h,i,j,k,l,m,n,o',names=nombre_columnas, nrows=tamaño_secciones)  
        print(df)
        print("=================================")

        for iteracacion in range(2,25):

            print("=========================")
            df = pd.read_excel(path_archivo,sheet_name=5,skiprows= tamaño_secciones * iteracacion + (var_aux + (2 * (iteracacion - 1))), usecols='e,f,g,h,i,j,k,l,m,n,o',names=nombre_columnas, nrows=tamaño_secciones)  
            print(df)
            print("=================================")


    def construccion_datframes(self,tamaño_secciones,path_archivo,nombres_columnas):
        
        """
        Para construir los dataframe seguimos los siguientes pasos:
        1 - Creamos un dataframe que contendra todos los datos en bruto.
        2 - Recorremos cada seccion del dataframe, iterando y detectando cada numero de FILA que corresponde a una seccion nueva.
        2.1 - En cada iteraccion asignamos numero de provincia y fechas.

        El dataframe resultante es una concatenacion de todas las secciones, con clave de provincia, y un campo mas, la fecha.
        """
        
        # ==== PASO 1 - Construccion del dataframe GENERAl
        nombre_columnas = ['provincia','fecha','bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
                'articulo_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']

        df = pd.read_excel(path_archivo,sheet_name=5,skiprows= 4,usecols='a,c,e,f,g,h,i,j,k,l,m,n,o',names=nombre_columnas)

        # ===# PASO 2 - Recorrido del dataframe

        #Dataframe que contendra todos los datos
        df_provincias = pd.DataFrame(columns = nombre_columnas)
        
        #La lista esta compuesta por (Nombre de pronvicia, numero de ID de la BDD)
        lista_provincias = [
            ['Ciudad Autónoma de Buenos Aires',2],
            ['24 partidos del Gran Buenos Aires ',6], #--> EL ESPACIO ES NECESARIO EN ESTA CADENA, VA A FINAL DESPUES DE 'Aires'
            ['Resto de Buenos Aires',6],
            ['Catamarca',10],
            ['Chaco',22],
            ['Chubut',26],
            ['Córdoba',14],
            ['Corrientes',18],
            ['Entre Ríos',30],
            ['Formosa',34],
            ['Jujuy',38],
            ['La Pampa',42],
            ['La Rioja',46],
            ['Mendoza',50],
            ['Misiones',54],
            ['Neuquén',58],
            ['Río Negro',62],
            ['Salta',66],
            ['San Juan',70],
            ['San Luis',74],
            ['Santa Cruz',78],
            ['Santa Fe',82],
            ['Santiago del Estero',86],
            ['Tierra del Fuego',94],
            ['Tucumán',90],
        ]


        #Generamos la lista de fecha que insertaremos en los dataframes
        fecha_inicio = date(2017, 1,1)
         #La cantidad de fechas, es la cantidad de filas por seccion

        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(tamaño_secciones)]

        for provincia in lista_provincias:
        
            # Buscar la fila y columna del valor específico
            fila, columna = df[df == provincia[0]].stack().index[0]

            fila = fila + 1 #--> Se suma uno por desfase

            #Con la fila detectada, extraemos la seccion que nos interesa y con el Cod. de pronvincia modificamos la columna 'provincia'
            df_por_provincia = df.iloc[fila : fila + tamaño_secciones]
            df_por_provincia['provincia'] = provincia[1]

            #Asignamos columna fecha
            df_por_provincia['fecha'] = lista_fechas

            df_provincias = pd.concat([df_provincias,df_por_provincia])
        

        print(df_provincias)
        

Transformation_Data().contruccion_df()

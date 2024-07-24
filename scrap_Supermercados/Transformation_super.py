#Objetivo: Obtener los datos del excel descargado, y brindarle el formato adecuado
import os
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
# Desactivar las advertencias
pd.options.mode.chained_assignment = None  # default='warn'

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

        #Recordatorio - Si yo quiero acceder a la fila 6 de un excel por ejemplo, hay que poner 2 filas menos siempre.
        df_aux = pd.read_excel(path_archivo,sheet_name=5,skiprows=5, usecols='c',names=nombre_columna)

        tamaño_secciones = self.construccion_lista_meses(df_aux['fecha']) #--> Obtenemos el tamaño que es la misma para cada pronvicia


        nombre_columnas = ['id_provincia_indec','fecha','total_facturacion','bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
                'articulos_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']
        
        return self.construccion_datframes(tamaño_secciones,path_archivo,nombre_columnas)
        

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
        nombre_archivo = '/files/encuesta_supermercado.xls'
        path_archivo = directorio_actual + nombre_archivo
        return path_archivo


    def construccion_datframes(self,tamaño_secciones,path_archivo,nombres_columnas):
        
        """
        Para construir los dataframe seguimos los siguientes pasos:
        1 - Creamos un dataframe que contendra todos los datos en bruto.
        2 - Recorremos cada seccion del dataframe, iterando y detectando cada numero de FILA que corresponde a una seccion nueva.
        2.1 - En cada iteraccion asignamos numero de provincia y fechas.
        3 - Transformaciones y finales y retorno

        El dataframe resultante es una concatenacion de todas las secciones, con clave de provincia, y un campo mas, la fecha.
        """
        
        # ==== PASO 1 - Construccion del dataframe GENERAl
        df = pd.read_excel(path_archivo,sheet_name=5,skiprows= 2,usecols='a,c,d,e,f,g,h,i,j,k,l,m,n,o',names=nombres_columnas)

        # ===# PASO 2 - Recorrido del dataframe

        #Dataframe que contendra todos los datos
        df_provincias = pd.DataFrame(columns = nombres_columnas)
        

        #La lista esta compuesta por (Nombre de pronvicia, numero de ID de la BDD, ID region)
        lista_provincias = [
            ['Total',1,1],
            ['Ciudad Autónoma de Buenos Aires',2,2],
            ['24 partidos del Gran Buenos Aires ',6,2], #--> EL ESPACIO ES NECESARIO EN ESTA CADENA, VA A FINAL DESPUES DE 'Aires'
            ['Resto de Buenos Aires',6,3],
            ['Catamarca',10,4],
            ['Chaco',22,5],
            ['Chubut',26,7],
            ['Córdoba',14,3],
            ['Corrientes',18,5],
            ['Entre Ríos',30,3],
            ['Formosa',34,5],
            ['Jujuy',38,4],
            ['La Pampa',42,3],
            ['La Rioja',46,4],
            ['Mendoza',50,6],
            ['Misiones',54,5],
            ['Neuquén',58,7],
            ['Río Negro',62,7],
            ['Salta',66,4],
            ['San Juan',70,6],
            ['San Luis',74,6],
            ['Santa Cruz',78,7],
            ['Santa Fe',82,3],
            ['Santiago del Estero',86,4],
            ['Tierra del Fuego',94,7],
            ['Tucumán',90,4],
        ]


        #Generamos la lista de fecha que insertaremos en los dataframes
        fecha_inicio = date(2017, 1,1)
         #La cantidad de fechas, es la cantidad de filas por seccion

        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(tamaño_secciones)]

        for provincia in lista_provincias:
        
            #Buscar la fila y columna del valor específico
            fila, columna = df[df == provincia[0]].stack().index[0]
            
            fila = fila + 1 #--> Se suma uno por desfase

            #Con la fila detectada, extraemos la seccion que nos interesa y con el Cod. de pronvincia modificamos la columna 'provincia'
            df_por_provincia = df.iloc[fila : fila + tamaño_secciones]
            df_por_provincia['id_provincia_indec'] = provincia[1]
            df_por_provincia['id_region_indec'] = int(provincia[2])

            #Asignamos columna fecha
            df_por_provincia['fecha'] = lista_fechas
            df_provincias = pd.concat([df_provincias,df_por_provincia])


        #==== PASO 3 - Pasos finales
            
        #Cambios algunos tipos de datos por omisiones de datos
        columnas_a_transformar = ['alimentos_preparados_rostiseria','indumentaria_calzado_textiles_hogar','electronica_hogar']

        df_provincias[columnas_a_transformar] = df_provincias[columnas_a_transformar].replace('s',None) #--> Cambios caracter 's' por None
        df_provincias[columnas_a_transformar] = df_provincias[columnas_a_transformar].applymap(lambda x: float(x) if pd.notnull(x) else None) #--> Transformamos la columna a flotante
        
        #Aplicamos redondeo al final para evitar 
        return df_provincias
    

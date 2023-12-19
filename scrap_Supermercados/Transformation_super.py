#Objetivo: Obtener los datos del excel descargado, y brindarle el formato adecuado
import os
import pandas as pd

class Transformation_Data:


    def contruccion_df(self):

        path_archivo = self.direccion_archivo()

        #nombre_columnas = ['fecha','bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
        #                  'articulo_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']
        #df = pd.read_excel(path_archivo,sheet_name=5,skiprows=5, usecols='c,e,f,g,h,i,j,k,l,m,n,o',names=nombre_columnas)

        """
        Vamos a rescatar una columna, no importa cual, y vamos a obtener el tamaño de la primera seccion
        Luego vamos a iterrar cada seccion teniendo en cuenta este tamaño. 

        El dataframe 'df_aux' se descarta luego de usarlo
        El tamaño se almacena en la variable 'tamaño_secciones'

        """
        nombre_columna = ['fecha']

        df_aux = pd.read_excel(path_archivo,sheet_name=5,skiprows=5, usecols='C',names=nombre_columna)

        tamaño_secciones = self.construccion_lista_meses(df_aux['fecha'])
        print(tamaño_secciones)

        nombre_columnas = ['bebidas','almacen','panaderia','lacteos','carnes','verduleria_fruteria','alimentos_preparados_rostiseria',
                          'articulo_limpieza_perfumeria','indumentaria_calzado_textiles_hogar','electronica_hogar','otros']
        df = pd.read_excel(path_archivo,sheet_name=5,skiprows=5, usecols='e,f,g,h,i,j,k,l,m,n,o',names=nombre_columnas, nrows=tamaño_secciones)  

        print(df)
   

    #Construimos una lista con todos los meses de la primera seccion
    def construccion_lista_meses(self,lista_fechas):


        lista_de_cadenas = [str(elemento) for elemento in lista_fechas]

        lista_retorno = []

        for valor in lista_de_cadenas:

            if valor == 'nan':
                break
            else:
                
                lista_retorno.append(valor)


        lista_retorno = [cadena.replace("*","") for cadena in lista_retorno]

        return len(lista_retorno)


    def direccion_archivo(self):

        # Direccion actual + nombre archivo = direccion del archivo
        directorio_actual  = os.path.dirname(os.path.abspath(__file__))
        nombre_archivo = '\\files\\encuesta_supermercado.xlsx'
        path_archivo = directorio_actual + nombre_archivo
        return path_archivo


Transformation_Data().contruccion_df()

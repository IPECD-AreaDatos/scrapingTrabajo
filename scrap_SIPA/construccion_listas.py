import numpy as np
import pandas as pd
import sys
import datetime
class ExtractData:
  
    #Objetivo: obtener los datos de puestos de trabajo por provincias
    def listado_provincias(self, file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas):
        
        try:

            # ==== DATAFRAME DE DATOS ESTACIONALES
            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=13, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   

            df = df.iloc[:-5]#Elimina las ultimas 5 filas siempre

            df.drop(df.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df = df.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2009-01-01')
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='MS').date



            # ==== DATAFRAME DE DATOS NO ESTACIONALES


            #Leer el archivo Excel en un DataFrame de pandas
            df_NoEstacional = pd.read_excel(file_path, sheet_name=14, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df_NoEstacional = df_NoEstacional.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df_NoEstacional = df_NoEstacional.replace(',', '.', regex=True)   
            df_NoEstacional = df_NoEstacional.iloc[:-6]#Elimina las ultimas 6 filas siempre
            df_NoEstacional.drop(df_NoEstacional.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df_NoEstacional = df_NoEstacional.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2009-01-01')
            df_NoEstacional['Período'] = pd.date_range(start=start_date, periods=len(df_NoEstacional), freq='MS').date


            # Aquí 'row' corresponde a la fila actual del dataframe 'df'
            # Y 'row_no_estacional' corresponde a la fila actual del dataframe 'df_NoEstacional'

            for (_, row), (_, row_no_estacional) in zip(df.iterrows(), df_NoEstacional.iterrows()):

                #Obtencion de fecha - Esta debe agregarse varias veces para diferentes provinicias
                fecha = row['Período']

                
                # ========================================================================================# 
                
                #Carga de valores BUENOS AIRES - ID = 6
                valor_estacionalidad = row['BUENOS AIRES'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['BUENOS AIRES'] # Valor no estacional
                
                #Valores obtenidos se agregan a las listas - Cargamos BUENOS AIRES
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(6) 
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                
                print("==================================================================")
                print("Provincia: Buenos Aires")
                print("Numero provincia: 6 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

                
                # ========================================================================================# 


                #Carga de valores CABA - ID = NO SE TODAVIA
                valor_estacionalidad = row['Cdad. Autónoma \nde Buenos Aires'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Cdad. Autónoma\nde Buenos Aires'] # Valor no estacional
                
                #Valores obtenidos se agregan a las listas - Cargamos CABA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(2) 
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                                
                print("==================================================================")
                print("Provincia: CABA")
                print("Numero provincia: 2 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

                
                # ========================================================================================# 

                #Carga de valores CATAMARCA - ID = no se todavia   
                valor_estacionalidad = row['CATAMARCA']
                valor_sin_estacionalidad = row_no_estacional['CATAMARCA']

                #Valores obtenidos se agregan a las listas - Cargamos CATAMARCA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(10)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Catamarca")
                print("Numero provincia: 10 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

                # ========================================================================================# 


                #Carga de valores CHACO - ID = no se todavia   
                valor_estacionalidad = row['CHACO']
                valor_sin_estacionalidad = row_no_estacional['CHACO']

                #Valores obtenidos se agregan a las listas - Cargamos CHACO
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(22)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Chaco")
                print("Numero provincia: 22 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")


                # ========================================================================================# 

                #Carga de valores CHUBUT - ID = no se todavia   
                valor_estacionalidad = row['CHUBUT']
                valor_sin_estacionalidad = row_no_estacional['CHUBUT']

                #Valores obtenidos se agregan a las listas - Cargamos CHUBUT
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(26)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Chubut")
                print("Numero provincia: 26 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

               # ========================================================================================# 

                #Carga de valores CORDOBA - ID = no se todavia   
                valor_estacionalidad = row['CÓRDOBA']
                valor_sin_estacionalidad = row_no_estacional['CÓRDOBA']

                #Valores obtenidos se agregan a las listas - Cargamos CORDOBA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(14)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Cordoba")
                print("Numero provincia: 14 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")
                
              # ========================================================================================# 

                #Carga de valores CORRIENTES - ID = 18
                valor_estacionalidad = row['CORRIENTES']
                valor_sin_estacionalidad = row_no_estacional['CORRIENTES']

                #Valores obtenidos se agregan a las listas - Cargamos CORRIENTES
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(18)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Corrientes")
                print("Numero provincia: 18 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores ENTRE RÍOS - ID = no se todavia   
                valor_estacionalidad = row['ENTRE RÍOS']
                valor_sin_estacionalidad = row_no_estacional['ENTRE RÍOS']

                #Valores obtenidos se agregan a las listas - Cargamos ENTRE RIOS
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(30)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Entre Rios")
                print("Numero provincia: 30 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores FORMOSA - ID = no se todavia   
                valor_estacionalidad = row['FORMOSA']
                valor_sin_estacionalidad = row_no_estacional['FORMOSA']

                #Valores obtenidos se agregan a las listas - Cargamos FORMOSA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(34)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Formosa")
                print("Numero provincia: 34 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores JUJUY - ID = no se todavia   
                valor_estacionalidad = row['JUJUY']
                valor_sin_estacionalidad = row_no_estacional['JUJUY']

                #Valores obtenidos se agregan a las listas - Cargamos JUJUY
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(38)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Jujuy")
                print("Numero provincia: 38 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")
    
              # ========================================================================================# 

                #Carga de valores LA PAMPA - ID = no se todavia   
                valor_estacionalidad = row['LA PAMPA']
                valor_sin_estacionalidad = row_no_estacional['LA PAMPA']

                #Valores obtenidos se agregan a las listas - Cargamos LA PAMPA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(42)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: La Pampa")
                print("Numero provincia: 42 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores LA RIOJA- ID = no se todavia   
                valor_estacionalidad = row['LA RIOJA']
                valor_sin_estacionalidad = row_no_estacional['LA RIOJA']

                #Valores obtenidos se agregan a las listas - Cargamos LA RIOJA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(46)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: La Rioja")
                print("Numero provincia: 46 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores MENDOZA- ID = no se todavia   
                valor_estacionalidad = row['MENDOZA']
                valor_sin_estacionalidad = row_no_estacional['MENDOZA']

                #Valores obtenidos se agregan a las listas - Cargamos MENDOZA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(50)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Mendoza")
                print("Numero provincia: 50 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores MISIONES - ID = no se todavia   
                valor_estacionalidad = row['MISIONES']
                valor_sin_estacionalidad = row_no_estacional['MISIONES']

                #Valores obtenidos se agregan a las listas - Cargamos MISIONES
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(54)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Misiones")
                print("Numero provincia: 54 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores NEUQUÉN- ID = no se todavia   
                valor_estacionalidad = row['NEUQUÉN']
                valor_sin_estacionalidad = row_no_estacional['NEUQUÉN']

                #Valores obtenidos se agregan a las listas - Cargamos NEUQUÉN
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(58)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Neuquen")
                print("Numero provincia: 58 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores RÍO NEGRO- ID = no se todavia   
                valor_estacionalidad = row['RíO NEGRO']
                valor_sin_estacionalidad = row_no_estacional['RÍO NEGRO']

                #Valores obtenidos se agregan a las listas - Cargamos RÍO NEGRO
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(62)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Rio Negro")
                print("Numero provincia: 62 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SALTA- ID = no se todavia   
                valor_estacionalidad = row['SALTA']
                valor_sin_estacionalidad = row_no_estacional['SALTA']

                #Valores obtenidos se agregan a las listas - Cargamos SALTA
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(66)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Salta")
                print("Numero provincia: 66 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SAN JUAN- ID = no se todavia   
                valor_estacionalidad = row['SAN JUAN']
                valor_sin_estacionalidad = row_no_estacional['SAN JUAN']

                #Valores obtenidos se agregan a las listas - Cargamos SAN JUAN
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(70)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: San Juan")
                print("Numero provincia: 70 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SAN LUIS- ID = no se todavia   
                valor_estacionalidad = row['SAN LUIS']
                valor_sin_estacionalidad = row_no_estacional['SAN LUIS']

                #Valores obtenidos se agregan a las listas - Cargamos SAN LUIS
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(74)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: San Luis")
                print("Numero provincia: 74 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SANTA CRUZ- ID = no se todavia   
                valor_estacionalidad = row['SANTA CRUZ']
                valor_sin_estacionalidad = row_no_estacional['SANTA CRUZ']

                #Valores obtenidos se agregan a las listas - Cargamos SANTA CRUZ
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(78)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Santa Cruz")
                print("Numero provincia: 78 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SANTA FE- ID = no se todavia   
                valor_estacionalidad = row['SANTA FE']
                valor_sin_estacionalidad = row_no_estacional['SANTA FE']

                #Valores obtenidos se agregan a las listas - Cargamos SANTA FE
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(82)
                lista_fechas.append(fecha)
                lista_registro.append(1) 

                print("==================================================================")
                print("Provincia: Santa Fe")
                print("Numero provincia: 82 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores SANTIAGO \nDEL ESTERO - ID = no se todavia   
                valor_estacionalidad = row['SANTIAGO \nDEL ESTERO']
                valor_sin_estacionalidad = row_no_estacional['SANTIAGO \nDEL ESTERO']

                #Valores obtenidos se agregan a las listas - Cargamos SANTIAGO \nDEL ESTERO
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(86)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Santiago del Estero")
                print("Numero provincia: 86 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

              # ========================================================================================# 

                #Carga de valores TIERRA DEL FUEGO - ID = no se todavia   
                valor_estacionalidad = row['TIERRA DEL FUEGO']
                valor_sin_estacionalidad = row_no_estacional['TIERRA DEL FUEGO']

                #Valores obtenidos se agregan a las listas - Cargamos TIERRA DEL FUEGO
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(94)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Tierra del Fuego")
                print("Numero provincia: 94 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")
                
              # ========================================================================================# 

                #Carga de valores TUCUMÁN - ID = no se todavia   
                valor_estacionalidad = row['TUCUMÁN']
                valor_sin_estacionalidad = row_no_estacional['TUCUMÁN']

                #Valores obtenidos se agregan a las listas - Cargamos TUCUMÁN
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(90)
                lista_fechas.append(fecha)
                lista_registro.append(1)  

                print("==================================================================")
                print("Provincia: Tucuman")
                print("Numero provincia: 90 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")

    #Objetivo: obteners los datos de puestro de trabajo del total de la nacion 
    def listado_nacion(self, file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas):


        try:

            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=3, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   
            df = df.iloc[:-6]#Elimina las ultimas 6 filas siempre
            df.drop(df.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df = df.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2012-01-01')
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='MS').date

 
            
            #Leer el archivo Excel en un DataFrame de pandas
            df_NoEstacional = pd.read_excel(file_path, sheet_name=4, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df_NoEstacional = df_NoEstacional.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df_NoEstacional = df_NoEstacional.replace(',', '.', regex=True)   
            df_NoEstacional = df_NoEstacional.iloc[:-8]#Elimina las ultimas 6 filas siempre
            df_NoEstacional.drop(df_NoEstacional.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df_NoEstacional = df_NoEstacional.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2012-01-01')
            df_NoEstacional['Período'] = pd.date_range(start=start_date, periods=len(df_NoEstacional), freq='MS').date
            
            for (_, row), (_, row_no_estacional) in zip(df.iterrows(), df_NoEstacional.iterrows()):


                #Obtencion de fecha - Esta debe agregarse varias veces para diferentes categorias
                fecha = row['Período']

            # ========================================================================================# 

                #Emplo Asalariado en el SECTOR PRIVADO - Codigo 1

                valor_estacionalidad = row['Empleo asalariado en el sector privado'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Empleo asalariado en el sector privado'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(2)
                lista_fechas.append(fecha)
               
                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")
               
           # ========================================================================================# 

                #Emplo Asalariado en SECTOR PUBLICO - codigo 2

                valor_estacionalidad = row['Empleo asalariado en el sector público'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Empleo asalariado en el sector público'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia               
                lista_registro.append(3)
                lista_fechas.append(fecha)

                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

           # ========================================================================================# 

                #Emplo EN CASAS PARTICULARES - codigo 3

                valor_estacionalidad = row['Empleo en casas particulares'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Empleo en casas particulares'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(4)
                lista_fechas.append(fecha)

                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

           # ========================================================================================# 

                #TRABAJO INDEPENDIENTES AUTONOMOS - codigo 4

                valor_estacionalidad = row['Trabajo Independientes Autónomos'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Trabajo Independientes Autónomos'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(5)
                lista_fechas.append(fecha)
                
                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

           # ========================================================================================# 

                #TRABAJO INDEPENDIENTES MONOTRIBUTO - codigo 5

                valor_estacionalidad = row['Trabajo Independientes Monotributo'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Trabajo Independientes Monotributo'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(6)
                lista_fechas.append(fecha)

                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

           # ========================================================================================# 

                #Trabajo Independientes Monotributo\nSocial - codigo 6

                valor_estacionalidad = row['Trabajo Independientes Monotributo\nSocial'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Trabajo Independientes Monotributo\nSocial'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(7)
                lista_fechas.append(fecha)

                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")

           # ========================================================================================# 

                #Total - codigo 7

                valor_estacionalidad = row['Total'] # Valor estacional
                valor_sin_estacionalidad = row_no_estacional['Total'] # Valor no estacional

                #Carga de datos
                lista_valores_estacionalidad.append(valor_estacionalidad)
                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                lista_provincias.append(1)#Provincia
                lista_registro.append(8)
                lista_fechas.append(fecha)

                print("==================================================================")
                print("Nacion")
                print("Numero: 1 ")
                print("Fecha: ", fecha)
                print("Valores con estacionalidad ", valor_estacionalidad)
                print("Valores sin estacionalidad ", valor_sin_estacionalidad)
                print("==================================================================")
    
        except Exception as e:
            
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")



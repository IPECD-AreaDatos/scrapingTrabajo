import numpy as np
import pandas as pd


class LoadXLSTrabajoRegistrado:
    def loadInDataBase(self, file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas):


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
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='M').date
            
            #Leer el archivo Excel en un DataFrame de pandas
            df_NoEstacional = pd.read_excel(file_path, sheet_name=4, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df_NoEstacional = df_NoEstacional.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df_NoEstacional = df_NoEstacional.replace(',', '.', regex=True)   
            df_NoEstacional = df_NoEstacional.iloc[:-9]#Elimina las ultimas 6 filas siempre
            df_NoEstacional.drop(df_NoEstacional.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df_NoEstacional = df_NoEstacional.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2012-01-01')
            df_NoEstacional['Período'] = pd.date_range(start=start_date, periods=len(df_NoEstacional), freq='M').date
            

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




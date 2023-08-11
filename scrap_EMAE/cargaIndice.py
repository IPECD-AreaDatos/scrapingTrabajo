import time
import numpy as np
import pandas as pd
import xlrd
class cargaIndice:
    def loadXLSIndiceEMAE(self, file_path, lista_fechas, lista_SectorProductivo, lista_valores):

        try:

            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=0, skiprows=2)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Obtener las columnas desde C hasta R
            columnas_valores = df.columns[2:18]  # Columnas C a R
            
            # Iterar a través de las filas a partir de la fila 6
            for index, row in df.iterrows():
                if index >= 5:  # Fila 6 en adelante
                    lista_fechas.append(row[0])  # Agregar la fecha a la lista
                    
                    for columna_valor in columnas_valores:
                        valor_celda = row[columna_valor]
                        if valor_celda is not None:
                            lista_valores.append(valor_celda)  # Agregar valor a la lista de valores
                            
                            # Obtener el valor correspondiente de la fila 3 y la columna actual
                            sector_productivo = df.at[2, columna_valor]
                            lista_SectorProductivo.append(sector_productivo)
            

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
    



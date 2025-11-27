import numpy as np
import pandas as pd
import sys
import datetime
import os
from sqlalchemy import create_engine
from unidecode import unidecode

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'ev_remun_trab_reg_por_sector.xlsx')

class Transformacion:

    #DEfinicion de atributos
    def __init__(self, host, user, password, database):
                
        #Conectamos a la BDD
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/{database}")
        
        #Diccionario con el que realizaremos codificacion
        self.diccionario = None

    # Construir el diccionario con id_categoria, id_subcategoria y nombre
    def construir_dic(self):

        # Leer la tabla oede_diccionario de la base de datos
        tabla = pd.read_sql_query("SELECT * FROM OEDE_diccionario", con=self.engine)

        # Crear una columna 'codigo'
        tabla['codigo'] = tabla.apply(lambda row: [row['id_categoria'], int(row['id_subcategoria'])], axis=1)

        # Crear un diccionario original utilizando el nombre como clave
        diccionario_original = {}
        for index, row in tabla.iterrows():
            clave = self.formatear_key(row['nombre'])
            value = [row['id_categoria'], int(row['id_subcategoria'])]
            
            if clave in diccionario_original:
                diccionario_original[clave].append(value)  # Almacenar múltiples valores
            else:
                diccionario_original[clave] = [value]  # Crear una nueva lista

        # Crear el diccionario final manteniendo todos los valores
        self.diccionario = diccionario_original  # Almacena todos los valores

    #Objetivo: formatear las keys del diciconario generado || Tambien se usa para formatear las subdivisiones que se presentan en el excel
    def formatear_key(self,key):
        # Convertir a minúsculas
        key = key.lower()
        # Eliminar tildes y acentos
        key = unidecode(key)
        # Eliminar comas, puntos y espacios
        key = key.replace(',', '').replace('.', '').replace(' ', '')
        return key
    
    #Objetivo: construir el df de la provincia pasada por parametro 
    def construir_df(self, provincia, id):

        #==== OBTENCION DE LOS DATOS del excel
        df = pd.read_excel(file_path_desagregado, sheet_name=provincia, skiprows=3)  # Leer el archivo XLSX y crear el DataFrame
        df = df.iloc[:70]
        df = df.drop(columns=['Rama de Actividad'])

        #Cambiamos el nombre de la primera columna, de Total nacional a claves_listas
        df.rename(columns={'Unnamed: 1': 'claves_listas'}, inplace=True)
        
        #==== FORMATEO DE DICCIONARIO
        df['claves_listas'] = df['claves_listas'].apply(self.formatear_key)

        # Función personalizada para manejar claves repetidas
        def mapear_clave(clave, index):
            # Forzar cualquier clave relacionada con 'calzado' a 'calzadoycuero'
            if 'calzado' in clave or 'cuero' in clave:
                return ['D', 19]  # Asignar directamente el código D 19 para 'calzadoycuero'
           
            # Forzar cualquier clave relacionada con 'edicion' a 'edicioneimpresion'
            if 'edicion' in clave:
                return ['D', 22]  # Asignar directamente el código D 22 para 'edicioneimpresion'
    
            # Obtener la lista de posibles coincidencias del diccionario
            valores = self.diccionario.get(clave, [[None, None]])
            
            # Si hay más de un valor, usar una regla para seleccionar cuál corresponde
            # En este ejemplo, usamos el índice (o alguna otra lógica) para seleccionar el correcto
            if len(valores) > 1:
                # Aquí podrías aplicar la lógica adecuada para elegir entre varios valores
                if 'alguna_columna' in df.columns:  # Ejemplo: si tienes otra columna que te ayude a elegir
                    valor_adicional = df.loc[index, 'alguna_columna']
                    if valor_adicional == 'condicion_1':
                        return valores[0]
                    else:
                        return valores[1]
                else:
                    # se elige según el índice.
                    return valores[1] if index >= 40 else valores[0]  # Ejemplo condicional
            else:
                return valores[0]  # Si no hay conflicto, devolver el único valor disponible

        # Mapeo de los datos utilizando el diccionario, manejando casos en que el valor no se encuentre en el diccionario
        df[['id_categoria', 'id_subcategoria']] = df.apply(
            lambda row: pd.Series(mapear_clave(row['claves_listas'], row.name)),
            axis=1)

        # Eliminar la columna original de claves_listas ya que ya no se necesita
        df = df.drop('claves_listas', axis=1)
        
        # Realizamos la transposición usando pd.melt() para las columnas de fechas
        df_transpuesto = pd.melt(
            df, 
            id_vars=['id_categoria', 'id_subcategoria'],  # Columnas que permanecerán como identificadores
            var_name='fecha',  # Nombre de la nueva columna que contendrá las fechas
            value_name='valor'  # Nombre de la columna donde irán los valores
        )

        # Convertimos la columna 'fecha' a tipo datetime para facilitar el análisis y 'valor' a float
        df_transpuesto['fecha'] = pd.to_datetime(df_transpuesto['fecha'], errors='coerce')
        df_transpuesto['valor'] = pd.to_numeric(df_transpuesto['valor'], errors='coerce')
        
        # Asignamos el valor de id_provincia pasado por parametro
        df_transpuesto['id_provincia'] = id

        # Reordenar las columnas en el DataFrame transpuesto
        df_transpuesto = df_transpuesto[['fecha', 'id_provincia', 'id_categoria', 'id_subcategoria', 'valor']]

        return df_transpuesto
    
    #Objetivo: armar un df por cada provincia para luego unirlos en uno solo
    def construir_dfs(self):
        # Diccionario que relaciona provincias con sus IDs
        provincias_dict = {
            'GBA': 1, 'CABA': 2, 'Resto Pcia. Bs. As.': 6, 'Catamarca': 10, 'Córdoba': 14, 
            'Corrientes': 18, 'Chaco': 22, 'Chubut': 26, 'Entre Ríos': 30, 'Formosa': 34, 
            'Jujuy': 38, 'La Pampa': 42, 'La Rioja': 46, 'Mendoza': 50, 'Misiones': 54, 
            'Neuquén': 58, 'Río Negro': 62, 'Salta': 66, 'San Juan': 70, 'San Luis': 74, 
            'Santa Cruz': 78, 'Santa Fe': 82, 'Santiago del Estero': 86, 'Tierra del Fuego': 90, 'Tucumán': 94
        }
        # Lista para almacenar los DataFrames de cada provincia
        dfs_provincias = []

        # Iterar por el diccionario para obtener provincia y su respectivo ID
        for provincia, id_provincia in provincias_dict.items():
            print(f"Armando el df de: {provincia} con ID: {id_provincia}")
            df = self.construir_df(provincia, id_provincia)
            dfs_provincias.append(df)  # Agregar el DataFrame resultante a la lista
            print(df)

        print(f"Cantidad de DataFrames en la lista: {len(dfs_provincias)}")

        # Concatenar todos los DataFrames en uno solo
        df_final = pd.concat(dfs_provincias, ignore_index=True)

        return df_final

    def main(self):
        self.construir_dic()
        df = self.construir_dfs()
        return df
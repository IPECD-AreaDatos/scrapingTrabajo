import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sys
import datetime
import os
from unidecode import unidecode

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'ev_remun_trab_reg_por_sector.xlsx')

class Diccionario:

    #DEfinicion de atributos
    def __init__(self, host, user, password, database):
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/{database}")
    
    #Lee el df del excel
    def leer(self):
        df = pd.read_excel(file_path_desagregado, sheet_name='GBA', skiprows=3)  # Leer el archivo XLSX y crear el DataFrame
        df = df.iloc[:, :2] 
        df = df.iloc[:70]
        return df
    
    #Objetivo: formatear las keys del diciconario generado || Tambien se usa para formatear las subdivisiones que se presentan en el excel
    def formatear_key(self,key):
        # Convertir a minúsculas
        key = key.lower()
        # Eliminar tildes y acentos
        key = unidecode(key)
        # Eliminar comas, puntos y espacios
        key = key.replace(',', '').replace('.', '').replace(' ', '')
        return key
    
    #Objetivo: identificar las categorias junto con su id para armar una tabla
    def crear_tabla_cat(self,df):
        # Listas para almacenar categorías y nombres
        categorias = []
        nombres = []

        # Iterar por cada fila del df
        for index, row in df.iterrows():
            # Si la primera celda es una letra entonces es una categoría
            if isinstance(row[0], str) and row[0].isalpha():
                categorias.append(row[0])  # Guardar la letra
                # Aplicar formateo al nombre de la categoría
                nombres.append(self.formatear_key(row[1]))  # Guardar el nombre formateado

        # Crear el df con categorías y nombres
        df_categorias = pd.DataFrame({
            'id_categoria': categorias,
            'categoria': nombres
        })
        return df_categorias

    #Objetivo: identificar las subcategorias junto con su id y su categoria para armar una tabla
    def crear_tabla_sub(self, df):
        categorias = []
        subcategorias = []
        nombres_subcategorias = []

        categoria_actual = None

        # Iterar por cada fila del df
        for index, row in df.iterrows():
            # Si la primera celda es una letra, es una categoría
            if isinstance(row[0], str) and row[0].isalpha():
                categoria_actual = row[0]  # Actualizar la categoría actual
            # Si la primera celda es un número, es una subcategoría
            elif isinstance(row[0], (int, float)):
                categorias.append(categoria_actual)  # Asociar con la categoría actual
                subcategorias.append(row[0])  # Guardar el número de subcategoría
                # Aplicar formateo al nombre de la subcategoría
                nombres_subcategorias.append(self.formatear_key(row[1]))  # Guardar el nombre formateado

        # Crear el df de subcategorías
        df_subcategorias = pd.DataFrame({
            'id_categoria': categorias,
            'id_subcategoria': subcategorias,
            'subcategoria': nombres_subcategorias
        })
        return df_subcategorias
       
    # Construir el diccionario con id_categoria, id_subcategoria y nombre
    def crear_dicc(self, df_categorias, df_subcategorias):

        # Obtener las tablas de categorías y subcategorías
        tabla_categorias = df_categorias
        tabla_subcategorias = df_subcategorias

        # Crear diccionario con id_categoria y nombre de las categorías, formateando la clave
        diccionario_categorias = {
            f"cat_{self.formatear_key(row['categoria'])}": [row['id_categoria'], None, row['categoria']]
            for index, row in tabla_categorias.iterrows()
        }

        # Crear diccionario con id_categoria, id_subcategoria y nombre de las subcategorías, formateando la clave
        diccionario_subcategorias = {
            f"sub_{self.formatear_key(row['subcategoria'])}": [row['id_categoria'], row['id_subcategoria'], row['subcategoria']]
            for index, row in tabla_subcategorias.iterrows()
        }

        # Combinar ambos diccionarios
        self.diccionario = {**diccionario_categorias, **diccionario_subcategorias}
        
        print(self.diccionario)

        # Convertir el diccionario a un DataFrame
        df = pd.DataFrame.from_dict(self.diccionario, orient='index', columns=['id_categoria', 'id_subcategoria', 'nombre'])

        # Rellenar los valores nulos (None) en 'id_subcategoria' con 0 o dejarlos como None según lo prefieras
        df['id_subcategoria'] = df['id_subcategoria'].fillna(0).astype(int)  # Si prefieres 0

        # Verificar el contenido del DataFrame
        print("DataFrame generado:")
        print(f"Total de filas: {len(df)}")
        print(df.columns)
        print(df.dtypes)
        return df

    # Subir el diccionario a la base
    def subir_dicc(self, df_dicc):
        try:
            # Subir la tabla de dicc.
            df_dicc.to_sql('OEDE_diccionario', con=self.engine, if_exists='replace', index=False)
            print("Tabla de diccionario subida exitosamente.")
        
        except Exception as e:
            print(f"Ocurrió un error al subir las tablas: {e}")

    def main(self):
        df = self.leer()
        df_categorias = self.crear_tabla_cat(df)
        df_subcategorias = self.crear_tabla_sub(df)
        df_diccionario = self.crear_dicc(df_categorias, df_subcategorias)
        self.subir_dicc(df_diccionario)

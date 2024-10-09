import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sys
import datetime
import os

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'ev_remun_trab_reg_por_sector.xlsx')

class Diccionario:

    def __init__(self, host, user, password, database):
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/{database}")

    def leer(self):
        df = pd.read_excel(file_path_desagregado, sheet_name='GBA', skiprows=3)  # Leer el archivo XLSX y crear el DataFrame
        df = df.iloc[:, :2] 
        df = df.iloc[:70]
        return df
    
    def crear_tabla_cat(self,df):
        # Listas para almacenar categorías y nombres
        categorias = []
        nombres = []

        # Iterar por cada fila del df
        for index, row in df.iterrows():
            # Si la primera celda es una letra entonces es una categoría
            if isinstance(row[0], str) and row[0].isalpha():
                categorias.append(row[0])  # Guardar la letra
                nombres.append(row[1])  # Guardar el nombre

        # Crear el df con categorías y nombres
        df_categorias = pd.DataFrame({
            'id_categoria': categorias,
            'categoria': nombres
        })

        print(df_categorias)

        return df_categorias

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
                nombres_subcategorias.append(row[1])  # Guardar el nombre de la subcategoría

        # Crear el df de subcategorías
        df_subcategorias = pd.DataFrame({
            'id_categoria': categorias,
            'id_subcategoria': subcategorias,
            'subcategoria': nombres_subcategorias
        })

        print(df_subcategorias)

        return df_subcategorias

    def subir_tablas(self, df_cat, df_sub):
        try:
            # Subir la tabla de categorías
            df_cat.to_sql('OEDE_categorias', con=self.engine, if_exists='replace', index=False)
            print("Tabla de categorías subida exitosamente.")

            # Subir la tabla de subcategorías
            df_sub.to_sql('OEDE_subcategorias', con=self.engine, if_exists='replace', index=False)
            print("Tabla de subcategorías subida exitosamente.")
        
        except Exception as e:
            print(f"Ocurrió un error al subir las tablas: {e}")

    def main(self):
        df = self.leer()
        df_categorias = self.crear_tabla_cat(df)
        df_subcategorias = self.crear_tabla_sub(df)
        self.subir_tablas(df_categorias,df_subcategorias)
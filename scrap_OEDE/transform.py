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

    #Objetivo: Construir el diccionario con el que que realizaremos el mapeo a nivel de subcategoria.
    def construir_dic(self):

        #Obtenemos tabla de subdivisiones
        tabla_subcategorias = pd.read_sql_query("SELECT * FROM OEDE_subcategorias", con=self.engine) 

        # Crear una columna 'codigo' combinando las columnas 'categoria' 'subcategoria' creando una lista con 2 valores 
        tabla_subcategorias['codigo'] = tabla_subcategorias.apply(lambda row: [(row['id_categoria']), int(row['id_subcategoria'])],axis=1)

        # Crear un diccionario de mapeo entre 'nombre' y la lista de 'codigo' que le corresponde
        self.diccionario = dict(zip(tabla_subcategorias['subcategoria'], tabla_subcategorias['codigo']))

        #Formateamos las keys, a un formato sin acentos, mayusculas, espacios, tildes
        self.diccionario  = {self.formatear_key(key): value for key, value in self.diccionario.items()}
        print(tabla_subcategorias)
        print(self.diccionario)

    #Objetivo: formatear las keys del diciconario generado || Tambien se usa para formatear las subdivisiones que se presentan en el excel
    def formatear_key(self,key):
        # Convertir a minúsculas
        key = key.lower()
        # Eliminar tildes y acentos
        key = unidecode(key)
        # Eliminar comas, puntos y espacios
        key = key.replace(',', '').replace('.', '').replace(' ', '')
        return key
    
    #Objetivo: construir el DF 
    def construir_df(self):

        #==== OBTENCION DE LOS DATOS del excel
        df = pd.read_excel(file_path_desagregado, sheet_name='GBA', skiprows=3)  # Leer el archivo XLSX y crear el DataFrame
        df = df.iloc[:70]
        df = df.drop(columns=['Rama de Actividad'])

        #Cambiamos el nombre de la primera columna, de Total nacional a claves_listas
        df.rename(columns={'Unnamed: 1': 'claves_listas'}, inplace=True)
        print(df['claves_listas'].isnull().sum())

        #==== FORMATEO DE DICCIONARIO
        df['claves_listas'] = df['claves_listas'].apply(self.formatear_key)

        #Realizamos el mapeo de los datos
        #df['claves_listas'] = df['claves_listas'].map(self.diccionario)

        #df[['id_categoria', 'id_subcategoria']] = pd.DataFrame(df['claves_listas'].tolist(), index=df.index)
        #df = df.drop('claves_listas',axis=1)
        print(df)

    def main(self):
        self.construir_dic()
        self.construir_df()

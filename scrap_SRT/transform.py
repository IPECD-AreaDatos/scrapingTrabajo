import pandas as pd
import numpy as np
import os
import sys
import csv

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')

class Transform:
    def __init__(self):
        # creamos el df final vacio
        self.df_final = pd.DataFrame()

    def procesar_archivos(self):
    
        # creamos una lista con todos los archivos .csv d la carpeta files
        archivos = [f for f in os.listdir(ruta_carpeta_files) if f.endswith(".csv")]

        print(archivos)

        for archivo in archivos:
            file_path = os.path.join(ruta_carpeta_files, archivo)
            self.normalizar_csv(file_path)

        # recorremos la lista de archivos .csv
        for archivo in archivos:
            file_path = os.path.join(ruta_carpeta_files, archivo)
            print(f"Procesando: {archivo}")

            # creamos el df del archivo
            df = self.crear_df(file_path)

             # Verificar si df es None o vacío
            if df is None or df.empty:
                print(f"⚠️ {archivo} fue omitido por estar vacío o con error.")
                continue

            # agregamos el df al final de nuestro df final
            self.df_final = pd.concat([self.df_final, df], ignore_index=True)

        return self.df_final
    
    # detectaremos si el archivo utiliza , o ; como separador
    def detectar_separador(self, file_path):

        with open(file_path, 'r', newline='', encoding='utf-8') as file:
            primera_linea = file.readline()
            if ',' in primera_linea and ';' in primera_linea:
                return ';'  # Puede ser que haya texto con comas, pero use ';' como separador principal
            elif ',' in primera_linea:
                return ','
            elif ';' in primera_linea:
                return ';'
            else:
                return ','
            
    # leer los archivos y guardarlos con , como separador para unificar
    def normalizar_csv(self, file_path):

        separador = self.detectar_separador(file_path)
        print(f"📂 Procesando {file_path} con separador detectado: '{separador}'")

        try:
            df = pd.read_csv(file_path, sep=separador, engine="python", dtype=str) 
            df.to_csv(file_path, index=False, sep=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')  
            print(f"✅ Archivo normalizado: {file_path}")
            return df
        except Exception as e:
            print(f"❌ Error procesando {file_path}: {e}")
            return None

    # generar el df limpio de un periodo
    def crear_df(self, file_path):

        df = pd.read_csv(file_path)
            
        # Eliminar espacios en los nombres de las columnas
        df.columns = df.columns.str.strip()
        
        # editamos las columnas
        df = self.columnass(df)

        # obtenemos la primer fecha y la pasamos al formato yy-mm-dd
        fecha = df.loc[5, 'periodo']
        año = int(fecha[:4])
        mes = int(fecha[4:6])  
        date = pd.Timestamp(year=año, month=mes, day=1).date()
        df['periodo'] = date
        df['periodo'] = pd.to_datetime(df['periodo'])

        # filtramos el df y generamos sumas
        df_agrupado = df.groupby(['periodo', 'jurisdiccion_desc', 'seccion', 'grupo', 'ciiu'], as_index=False).agg({
            'cant_personas_trabaj_up': 'sum',
            'remuneracion': 'sum'})
    
        # creamos una nueva columna resultado de dividir dos valores existentes
        df_agrupado['salario'] = df_agrupado['remuneracion'] / df_agrupado['cant_personas_trabaj_up']
        df_agrupado['salario'] = df_agrupado['salario'].fillna(0)  

        df_agrupado = self.provincias(df_agrupado)

        return df_agrupado
    
    # realizar modificaciones en las columnas del df
    def columnass(self, df):

        # cambiamos a minusculas todas las columnas
        df.columns = df.columns.str.lower()

        # reordenamos las columnas
        columnas = ['periodo', 'jurisdiccion_desc', 'seccion', 'grupo', 'ciiu', 'cant_personas_trabaj_up', 'remuneracion']

        # hacemos una copia del df
        df = df[columnas].copy()

        # convertimos a string algunas columnas
        columnas_str = ['periodo', 'jurisdiccion_desc', 'seccion', 'ciiu']
        df.loc[:, columnas_str] = df[columnas_str].astype(str)

        # convertimos a numerico las columnas
        df['remuneracion'] = pd.to_numeric(df['remuneracion'], errors='coerce')
        df['cant_personas_trabaj_up'] = pd.to_numeric(df['cant_personas_trabaj_up'], errors='coerce')

        return df
    
    # reemplazamos el nombre de la provincia por su id
    def provincias(self, df):

        # Diccionario para reemplazar provincias por sus códigos numéricos
        dict_provincias = {
            'C.A.B.A.': 2,
            'Buenos Aires': 6,
            'Catamarca': 10,
            'Chaco': 22,
            'Chubut': 26,
            'Cordoba': 14,
            'Corrientes': 18,
            'Entre Rios': 30,
            'Formosa': 34,
            'Jujuy': 38,
            'La Pampa': 42,
            'La Rioja': 46,
            'Mendoza': 50,
            'Misiones': 54,
            'Neuquen': 58,
            'Rio Negro': 62,
            'Salta': 66,
            'San Juan': 70,
            'San Luis': 74,
            'Santa Cruz': 78,
            'Santa Fe': 82,
            'Santiago del Estero': 86,
            'Sin datos': 0,
            'Tierra del Fuego': 94,
            'Tucuman': 90,
        }

        # Reemplazar los nombres de provincias por sus códigos numéricos
        df['jurisdiccion_desc'] = df['jurisdiccion_desc'].replace(dict_provincias)

        df = df.rename(columns={'jurisdiccion_desc': 'id_jurisdiccion'})
        
        df['id_jurisdiccion'] = pd.to_numeric(df['id_jurisdiccion'], errors='coerce')

        return df

"""
Transformer para procesar datos de CBA y CBT.

En este script manejamos dos hojas del mismo excel CBT.xls y extraemos datos del NEA desde Pobreza.xls:
"primera_hoja": corresponde a los datos de CBA y CBT de ADULTOS. Es decir, por individuo.
"segunda_hoja": corresponde a los datos de CBA y CBT de FAMILIAS. El tipo de familia es el 2, que es un grupo de 4 personas
"datos_nea": se extraen desde Pobreza.xls, hoja "Series canastas anexo", región Noreste
"""

import datetime
import time
import os
from openpyxl import load_workbook
import pandas as pd
import numpy as np


class TransformerCBTCBA:
    """
    Transformer para procesar y transformar datos de CBA, CBT y NEA.
    
    Procesa los archivos descargados y genera un DataFrame consolidado
    con todos los datos necesarios para el DataLake.
    """
    
    def transform_datalake(self):
        """
        Transforma los datos del DataLake.
        
        Returns:
            pd.DataFrame: DataFrame con los datos transformados
        """
        directorio_desagregado = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files', 'data')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_pobreza = os.path.join(ruta_carpeta_files, 'Pobreza.xls')

        # Datos de la primera hoja
        df_primeraHoja = pd.read_excel(file_path_desagregado, sheet_name=0, usecols=[0, 1, 3], skiprows=6, skipfooter=1)
        df_primeraHoja.columns = ['Fecha','CBA_Adulto','CBT_Adulto']
        
        df_primeraHoja = df_primeraHoja.sort_index()

        # Limpieza específica del valor incorrecto
        df_primeraHoja['CBA_Adulto'] = df_primeraHoja['CBA_Adulto'].astype(str).str.replace(',', '', regex=True)
        df_primeraHoja['CBT_Adulto'] = df_primeraHoja['CBT_Adulto'].astype(str).str.replace(',', '', regex=True)

        # Reemplazar el valor específico incorrecto
        df_primeraHoja.loc[df_primeraHoja['CBA_Adulto'] == '13874431', 'CBA_Adulto'] = '138744.31'
        df_primeraHoja.loc[df_primeraHoja['CBT_Adulto'] == '31217470', 'CBT_Adulto'] = '312174.70'

        # Convertir a numérico
        df_primeraHoja['CBA_Adulto'] = pd.to_numeric(df_primeraHoja['CBA_Adulto'], errors='coerce')
        df_primeraHoja['CBT_Adulto'] = pd.to_numeric(df_primeraHoja['CBT_Adulto'], errors='coerce')

        #Datos de la segunda hoja
        df_segundaHoja = pd.read_excel(file_path_desagregado, sheet_name=3, usecols=[2,6], skiprows=6, skipfooter=1)
        df_segundaHoja.columns = ['CBA_Hogar','CBT_Hogar']
        
        df_segundaHoja = df_segundaHoja.sort_index()

        # Eliminar filas completamente vacías (más seguro)
        df_primeraHoja = df_primeraHoja.dropna(how="all").reset_index(drop=True)
        df_segundaHoja = df_segundaHoja.dropna(how="all").reset_index(drop=True)

        # Extraer datos oficiales de CBA y CBT del NEA desde Pobreza.xls
        df_nea = self.extraer_datos_nea(file_path_pobreza, df_primeraHoja)

        #Transformacion de los datos --> Concatenar todo
        concatenacion_df = pd.concat([df_primeraHoja, df_segundaHoja, df_nea], axis=1)
        concatenacion_df['Fecha'] = pd.to_datetime(concatenacion_df['Fecha'])

        df_definitivo = self.estimaciones_nea(concatenacion_df)

        return df_definitivo
    

    def extraer_datos_nea(self, file_path_pobreza, df_fechas):
        """
        Extrae los datos de CBA y CBT del NEA desde el archivo Pobreza.xls
        
        Args:
            file_path_pobreza: Ruta al archivo Pobreza.xls
            df_fechas: DataFrame con las fechas de referencia
            
        Returns:
            DataFrame con columnas 'cba_nea' y 'cbt_nea'
        """
        try:
            # Leer la hoja "Series canastas anexo"
            df_raw = pd.read_excel(file_path_pobreza, sheet_name='Series canastas anexo', skiprows=5)
            
            # Encontrar todas las filas del NEA (Noreste)
            indices_nea = df_raw[df_raw.iloc[:, 0].astype(str).str.contains('Noreste', na=False, case=False)].index.tolist()
            
            if len(indices_nea) < 2:
                raise ValueError("No se encontraron suficientes filas del NEA en el archivo")
            
            # La primera aparición es CBA, la segunda es el coeficiente de Engel
            fila_nea_cba = indices_nea[0]
            fila_nea_engel = indices_nea[1]
            
            # Extraer valores de CBA del NEA (columnas 1 en adelante)
            valores_cba_nea = df_raw.iloc[fila_nea_cba, 1:].values
            
            # Extraer coeficientes de Engel del NEA
            coeficientes_engel = df_raw.iloc[fila_nea_engel, 1:].values
            
            # Calcular CBT = CBA * Coeficiente de Engel
            valores_cbt_nea = []
            for cba, engel in zip(valores_cba_nea, coeficientes_engel):
                if pd.notna(cba) and pd.notna(engel):
                    try:
                        valores_cbt_nea.append(float(cba) * float(engel))
                    except (ValueError, TypeError):
                        valores_cbt_nea.append(np.nan)
                else:
                    valores_cbt_nea.append(np.nan)
            
            # Crear DataFrame temporal con los datos disponibles del NEA
            df_nea_disponible = pd.DataFrame({
                'cba_nea': valores_cba_nea,
                'cbt_nea': valores_cbt_nea
            })
            
            # Convertir a numérico
            df_nea_disponible['cba_nea'] = pd.to_numeric(df_nea_disponible['cba_nea'], errors='coerce')
            df_nea_disponible['cbt_nea'] = pd.to_numeric(df_nea_disponible['cbt_nea'], errors='coerce')
            
            # Eliminar filas completamente vacías
            df_nea_disponible = df_nea_disponible.dropna(how='all').reset_index(drop=True)
            
            # Crear DataFrame final con la longitud correcta
            # Los datos del NEA solo están disponibles para los últimos meses
            # El resto debe ser NaN para que el método estimaciones_nea los calcule
            num_filas_total = len(df_fechas)
            num_datos_nea = len(df_nea_disponible)
            
            # Inicializar con NaN
            df_nea_final = pd.DataFrame({
                'cba_nea': [np.nan] * num_filas_total,
                'cbt_nea': [np.nan] * num_filas_total
            }, dtype='float64')
            
            # Colocar los datos del NEA al final (los más recientes)
            if num_datos_nea > 0:
                inicio = max(0, num_filas_total - num_datos_nea)
                df_nea_final.iloc[inicio:, :] = df_nea_disponible.iloc[:min(num_datos_nea, num_filas_total)].values
            
            return df_nea_final
            
        except Exception as e:
            print(f"[TRANSFORM] Error al extraer datos del NEA: {e}")
            import traceback
            traceback.print_exc()
            # En caso de error, devolver DataFrame vacío con la estructura correcta
            return pd.DataFrame({
                'cba_nea': [np.nan] * len(df_fechas),
                'cbt_nea': [np.nan] * len(df_fechas)
            }, dtype='float64')
    

    def estimaciones_nea(self,concatenacion_df):

        # Establecemos la fecha del ultimo periodo valido (los datos del NEA están disponibles hasta junio 2025)
        fecha_ultima_publicacion_oficial = pd.to_datetime("2025-06-01")

        # Verificacion para ver si es necesario o no calcular estimaciones del NEA
        # Se supone que el mismo mes que salen DATOS OFICIALES no se tiene que calcular
        if len(concatenacion_df['cba_nea'][concatenacion_df['Fecha'] > fecha_ultima_publicacion_oficial]) == 0:
            return concatenacion_df

        # En caso de que no estemos en el mismo mes de publicacion, se calculan las estimaciones
        # teniendo en cuenta los ultimos datos oficiales del NEA
        else:
            # En base a la fecha buscamos los ultimos 6 valores de CBA y CBT DE GBA, CBA y CBT de NEA
            df_sin_nulos = concatenacion_df[concatenacion_df['Fecha'] <= fecha_ultima_publicacion_oficial][-6:]

            # Filtrar valores None/NaN antes de sumar
            suma_cba = df_sin_nulos['CBA_Adulto'].dropna().sum()
            suma_cbt = df_sin_nulos['CBT_Adulto'].dropna().sum()
            suma_cba_nea = df_sin_nulos['cba_nea'].dropna().sum()
            suma_cbt_nea = df_sin_nulos['cbt_nea'].dropna().sum()
            
            # Verificar que las sumas no sean cero para evitar división por cero
            if suma_cba == 0 or suma_cbt == 0 or suma_cba_nea == 0 or suma_cbt_nea == 0:
                print("Advertencia: No hay suficientes datos del NEA para calcular estimaciones")
                return concatenacion_df
            
            # Inserción al dataframe de las estimaciones
            df_con_nulos = concatenacion_df[concatenacion_df['Fecha'] > fecha_ultima_publicacion_oficial]

            df_con_nulos['CBA_Adulto'] = pd.to_numeric(df_con_nulos['CBA_Adulto'], errors='coerce')
            df_con_nulos['CBT_Adulto'] = pd.to_numeric(df_con_nulos['CBT_Adulto'], errors='coerce')

            # Recorremos los datos nulos, calculamos sus estimaciones y finalmente los insertamos al DF definitivo
            for index, row in df_con_nulos.iterrows():
                
                # Calculos de las estimaciones
                estimacion_cba = row['CBA_Adulto'] * (suma_cba_nea / suma_cba)
                estimacion_cbt = row['CBT_Adulto'] * (suma_cbt_nea / suma_cbt)

                concatenacion_df.loc[index, 'cba_nea'] = estimacion_cba
                concatenacion_df.loc[index, 'cbt_nea'] = estimacion_cbt

            return concatenacion_df
        


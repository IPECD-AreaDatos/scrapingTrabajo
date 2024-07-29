import pandas as pd
import os


class Transform: 

    #Valores de los atributos
    def __init__(self):
    
        #DF de datos historicos
        self.df_historico = pd.DataFrame()

    def crear_df_precios_minoristas(self):
        
        #Direccion del ultimo archivo publicado
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'relevamiento_expectativas_mercado.xlsx')

        df = pd.read_excel(file_path_desagregado, skiprows=5)
        df = self.columnas(df)
        print(df.columns)
        print(df.dtypes)
        return df      
  
    def columnas(self, df):

        columnas_eliminar = ['Unnamed: 0', 'Referencia', 'Promedio', 'Desvío', 'Máximo','Mínimo', 'Percentil 90', 'Percentil 75', 'Percentil 25','Percentil 10', 'Cantidad de participantes']
        df = df.drop(columnas_eliminar, axis=1)
        df = df.iloc[:-94]
        df = df.rename(columns={'Período':'fecha_resguardo', 'Mediana':'mediana'})
        df.insert(1, 'fecha', df['fecha_resguardo'])
        df.insert(0, 'id', range(1, len(df) + 1))
        df['fecha_resguardo'] = df['fecha_resguardo'].astype(str)
        df['mediana'] = df['mediana'].astype(float)
        df.loc[len(df)-5:, 'fecha'] = ['2025-04-30 00:00:00', '2026-04-30 00:00:00', '2024-01-30 00:00:00', '2025-01-30 00:00:00', '2026-01-30 00:00:00']
        df['fecha'] = pd.to_datetime(df['fecha'])

        return df
    
    #Objetivo: obtener los datos del historial del REM - PRECIOS MINORISTAS
    def get_historico_precios_minoristas(self):

        #Direccion del archivo que contiene los datos historicos
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'historico_rem.xlsx')

        #Obtenemos el DATAFRAME
        df = pd.read_excel(file_path_desagregado,sheet_name=1,skiprows=1)

        #Filtrado: solo nos interesa los datos donde el campo 'Variable' = Precios minoristas (IPC nivel general; INDEC)
        df_var_mensuales = df[(df['Variable'] == 'Precios minoristas (IPC nivel general; INDEC)') & (df['Referencia'] == 'var. % mensual' )]

        #Seleccionar solo la PRIMEA y la QUINTA columna usando iloc
        #La primera columna (el 0) corresponde a la columna 'fecha de pronostico', y la cuarta columna (el 5) corresponde al campo 'mediana'
        df_seleccionado = df_var_mensuales.iloc[:, [0, 4]]
        df_seleccionado.columns = ['fecha','var_mensual']


        """
        El historico presenta datos divididos en bloques, cada bloque representa proyecciones a futuro del los proximos 6 meses.
        A lo largo de cada bloque, LA PRIMERA FILA de cada uno, NO APARECE EN LA SIGUIENTE. Por ejemplo:

        Bloque de ENERO: Proyecciones DESDE FEBRERO hasta JULIO
        Bloque de FEBRERO: Proyecciones DESDE  MARZO hasta AGOSTO

        Vemos que la Var. Mensual de febrero, no aparecera dos veces, por ende es necesario tomar, de cada bloque, solo la primera fila
        """

        df_seleccionado = df_seleccionado.drop_duplicates(subset=['fecha'])#--> Se eliminan los datos duplicados (toma la primera fecha de cada bloque)

        # Resetear el índice || SIRVE PARA LA CARGA
        df_seleccionado = df_seleccionado.reset_index(drop=True)

        return df_seleccionado
    

    #Objetivo: obtener los datos del historial del REM - CAMBIO NOMINAL
    def get_historico_cambio_nominal(self):

        #Direccion del archivo que contiene los datos historicos
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'historico_rem.xlsx')

        #Obtenemos el DATAFRAME
        df = pd.read_excel(file_path_desagregado,sheet_name=1,skiprows=1)

        #Filtrado: solo nos interesa los datos donde el campo 'Variable' = Tipo de cambio nominal
        df_var_mensuales = df[(df['Variable'] == 'Tipo de cambio nominal') & (df['Referencia'] == '$/USD' )]

        #Seleccionar solo la PRIMEA y la QUINTA columna usando iloc
        #La primera columna (el 0) corresponde a la columna 'fecha de pronostico', y la cuarta columna (el 5) corresponde al campo 'mediana'
        df_seleccionado = df_var_mensuales.iloc[:, [0, 4]]
        df_seleccionado.columns = ['fecha','cambio_nominal']


        """
        El historico presenta datos divididos en bloques, cada bloque representa proyecciones a futuro del los proximos 6 meses.
        A lo largo de cada bloque, LA PRIMERA FILA de cada uno, NO APARECE EN LA SIGUIENTE. Por ejemplo:

        Bloque de ENERO: Proyecciones DESDE FEBRERO hasta JULIO
        Bloque de FEBRERO: Proyecciones DESDE  MARZO hasta AGOSTO

        Vemos que la Var. Mensual de febrero, no aparecera dos veces, por ende es necesario tomar, de cada bloque, solo la primera fila
        """

        df_seleccionado = df_seleccionado.drop_duplicates(subset=['fecha'])#--> Se eliminan los datos duplicados (toma la primera fecha de cada bloque)

        # Resetear el índice || SIRVE PARA LA CARGA
        df_seleccionado = df_seleccionado.reset_index(drop=True)

        return df_seleccionado





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

        df_seleccionado_filtrado = df_seleccionado.drop_duplicates(subset=['fecha'])#--> Se eliminan los datos duplicados (toma la primera fecha de cada bloque)


        #Como tenemos que buscar el valor del proximo mes, tenemos que buscar los datos correspondiente a la ultima fecha
        #Que representa el ultimo bloque. Lo buscaremos en 'df_var_mensuales'
        fecha_max = max(df_seleccionado['fecha'])
        last_data = df_seleccionado[df_seleccionado['fecha'] == fecha_max]

        #Ahora tendriamos los datos de la ultima fecha, y el unico dato que nos interesa es la segunda fila, que representa
        #la prediccion de la inflacion al sig. mes del actual. Buscamos el dato y lo concatenamos
        #data_next_month = last_data.iloc[[1]]
        #data_next_month['fecha'] = pd.to_datetime(f'{fecha_max.year}-{fecha_max.month + 1}-01') #Le damos la fecha correcta del prox mes.

        # Manejar cambio de mes y año
        next_month = fecha_max.month + 1 if fecha_max.month < 12 else 1
        next_year = fecha_max.year if fecha_max.month < 12 else fecha_max.year + 1

        if len(last_data) > 1:
            data_next_month = last_data.iloc[[1]].copy()
            data_next_month.loc[:, 'fecha'] = pd.to_datetime(f'{next_year}-{next_month}-01')
        else:
            data_next_month = pd.DataFrame(columns=['fecha', 'var_mensual'])


        #Concatenamos al DF ORIGINAL
        df_seleccionado_filtrado = pd.concat([df_seleccionado_filtrado, data_next_month])
        df_seleccionado_filtrado = df_seleccionado_filtrado.sort_values(by='fecha').tail(12)

        # Resetear el índice || SIRVE PARA LA CARGA
        df_seleccionado_filtrado = df_seleccionado_filtrado.reset_index(drop=True)


        return df_seleccionado_filtrado
    

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

        df_seleccionado_filtrado = df_seleccionado.drop_duplicates(subset=['fecha'])#--> Se eliminan los datos duplicados (toma la primera fecha de cada bloque)


        #Como tenemos que buscar el valor del proximo mes, tenemos que buscar los datos correspondiente a la ultima fecha
        #Que representa el ultimo bloque. Lo buscaremos en 'df_var_mensuales'
        fecha_max = max(df_seleccionado['fecha'])
        last_data = df_seleccionado[df_seleccionado['fecha'] == fecha_max]

        #Ahora tendriamos los datos de la ultima fecha, y el unico dato que nos interesa es la segunda fila, que representa
        #la prediccion de la inflacion al sig. mes del actual. Buscamos el dato y lo concatenamos
        #data_next_month = last_data.iloc[[1]]
        #data_next_month['fecha'] = pd.to_datetime(f'{fecha_max.year}-{fecha_max.month + 1}-01') #Le damos la fecha correcta del prox mes.
        
        # Manejar cambio de mes y año
        next_month = fecha_max.month + 1 if fecha_max.month < 12 else 1
        next_year = fecha_max.year if fecha_max.month < 12 else fecha_max.year + 1

        if len(last_data) > 1:
            data_next_month = last_data.iloc[[1]].copy()
            data_next_month.loc[:, 'fecha'] = pd.to_datetime(f'{next_year}-{next_month}-01')
        else:
            data_next_month = pd.DataFrame(columns=['fecha', 'cambio_nominal'])

        #Concatenamos al DF ORIGINAL
        df_seleccionado_filtrado = pd.concat([df_seleccionado_filtrado, data_next_month])
        df_seleccionado_filtrado = df_seleccionado_filtrado.sort_values(by='fecha').tail(12)


        # Resetear el índice || SIRVE PARA LA CARGA
        df_seleccionado_filtrado = df_seleccionado_filtrado.reset_index(drop=True)


        return df_seleccionado

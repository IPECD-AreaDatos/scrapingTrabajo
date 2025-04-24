import pandas as pd
import os

class Transformer:

    #Definicion de atributos - Momentaneamente no es necesario
    def __init__(self):
        pass

    #Objetivo: construir el DF correspondiente a los valores del EMAE
    def construir_df_emae_valores(self):

        # Ruta del archivo
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'emae.xls')

        # Leer archivo
        df = pd.read_excel(file_path, sheet_name=0, skiprows=4)

        # Eliminar última fila si contiene "Fuente"
        if 'Fuente' in str(df.iloc[-1, 0]):
            df = df.drop(df.index[-1])

        # Rellenar los años hacia abajo
        df.iloc[:, 0] = df.iloc[:, 0].ffill()

        # Guardar nombres de sectores antes de eliminar columnas
        sectores = df.columns[2:].tolist()

        # Mapear meses en español a número
        meses = {
            'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04',
            'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08',
            'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'
        }

        # Construir columna de fechas
        df['anio'] = df.iloc[:, 0].astype(str)
        df['mes'] = df.iloc[:, 1].map(meses)
        df['fecha'] = pd.to_datetime(df['anio'] + '-' + df['mes'], format='%Y-%m', errors='coerce')

        # Eliminar filas con fechas inválidas
        df = df[df['fecha'].notna()]

        # Eliminar columnas originales
        df = df.drop(columns=[df.columns[0], df.columns[1], 'anio', 'mes'])

        # Reordenar columnas
        df = df[['fecha'] + [col for col in df.columns if col != 'fecha']]

        # Transformar a formato largo
        df_melted = df.melt(id_vars='fecha', var_name='sector', value_name='valor')

        # Asignar ID de sector numérico
        sector_map = {name: i+1 for i, name in enumerate(sectores)}
        df_melted['sector_productivo'] = df_melted['sector'].map(sector_map)

        # Eliminar valores nulos
        df_melted = df_melted.dropna(subset=['valor'])

        # Formatear y ordenar
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha']).dt.strftime('%Y-%m-%d')
        df_final = df_melted[['fecha', 'sector_productivo', 'valor']].sort_values(['fecha', 'sector_productivo'])

        return df_final



    #Objetivo: Construir el DF de las variaciones del EMAE
    def construir_df_emae_variaciones(self):
        # Ruta del archivo
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'emaevar.xls')

        # Leer el archivo
        df = pd.read_excel(file_path, sheet_name=0, skiprows=4, usecols="D,F")

        # Renombrar columnas
        df.columns = ['variacion_interanual', 'variacion_mensual']

        # Reemplazar NaN por 0 (o podrías eliminarlos si preferís)
        df = df.fillna(0)

        # Eliminar filas totalmente vacías (por si hay más de 2)
        df = df[(df != 0).any(axis=1)]

        # Generar fechas comenzando en febrero de 2004
        num_rows = df.shape[0]
        fechas = pd.date_range(start='2004-02-01', periods=num_rows, freq='MS')
        df['fecha'] = fechas

        # Reorganizar columnas
        df = df[['fecha', 'variacion_interanual', 'variacion_mensual']]

        #print(df)
        # ✅ Opcional: convertir a formato largo (como el otro)
        # df = df.melt(id_vars='fecha', var_name='tipo_variacion', value_name='valor')

        return df



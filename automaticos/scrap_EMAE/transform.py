import pandas as pd
import os

# Configurar pandas para evitar advertencias de deprecación
pd.set_option('future.no_silent_downcasting', True)

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

        # Leer archivo con skiprows=2 para obtener los headers correctos
        df = pd.read_excel(file_path, sheet_name=0, skiprows=2)
        
        # Eliminar filas que contengan "Fuente" en cualquier columna
        mask_fuente = df.astype(str).apply(lambda x: x.str.contains('Fuente', na=False)).any(axis=1)
        df = df[~mask_fuente]

        # La primera columna contiene los años, la segunda los meses
        year_col = df.columns[0]  # 'Período'
        month_col = df.columns[1]  # 'Unnamed: 1' (que contiene los meses)

        # Rellenar los años hacia abajo
        df[year_col] = df[year_col].ffill()

        # Obtener los nombres de los sectores (columnas desde la 2 en adelante)
        sectores = df.columns[2:].tolist()

        # Mapear meses en español a número
        meses = {
            'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04',
            'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08',
            'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'
        }

        # Construir columna de fechas
        # Convertir años a enteros para eliminar el .0, luego a string
        df['anio_str'] = df[year_col].fillna(0).astype(int).astype(str).replace('0', '')
        df['mes_num'] = df[month_col].map(meses)
        
        # Crear fechas solo para las filas que tienen año y mes válidos
        mask_valid = (
            df[year_col].notna() & 
            df[month_col].notna() & 
            df['mes_num'].notna() &
            (df['anio_str'] != '')  # Reemplazar la verificación de dígitos
        )
        
        df.loc[mask_valid, 'fecha'] = pd.to_datetime(
            df.loc[mask_valid, 'anio_str'] + '-' + df.loc[mask_valid, 'mes_num'], 
            format='%Y-%m', errors='coerce'
        )

        # Filtrar solo las filas con fechas válidas
        df = df[df['fecha'].notna()].copy()

        # Seleccionar solo las columnas necesarias (fecha + sectores)
        columnas_finales = ['fecha'] + sectores
        df = df[columnas_finales]

        # Transformar a formato largo
        df_melted = df.melt(id_vars='fecha', var_name='sector', value_name='valor')

        # Asignar ID de sector numérico
        sector_map = {name: i+1 for i, name in enumerate(sectores)}
        df_melted['sector_productivo'] = df_melted['sector'].map(sector_map)

        # Eliminar valores nulos en la columna valor
        df_melted = df_melted.dropna(subset=['valor'])

        # Formatear fechas y ordenar
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha']).dt.strftime('%Y-%m-%d')
        df_final = df_melted[['fecha', 'sector_productivo', 'valor']].sort_values(['fecha', 'sector_productivo'])

        return df_final.reset_index(drop=True)



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

        return df



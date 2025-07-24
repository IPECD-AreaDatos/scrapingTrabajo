import pandas as pd
from datetime import datetime
from etl_modular.utils.mappings import provincias
def transform_dnrpa_data(raw_data):
    if not raw_data:
        print("❌ No hay datos para transformar.")
        return pd.DataFrame()

    print("🔄 Transformando datos de DNRPA...")
    df_list = []

    for item in raw_data:
        anio = item['year']
        tipo_vehiculo = item['tipo_vehiculo']
        datos_tabla = item['table_data']

        df_formato_original = pd.DataFrame(datos_tabla)

        # La tabla tiene 26 filas de provincias, 2 de headers y 1 de totales.
        # Seleccionamos las filas de provincias, ignorando el header y el total.
        df_formato_original = df_formato_original.iloc[2:26, 0:13].copy()
        
        # Leemos los nombres de las provincias para el mapeo
        df_formato_original.columns = ['provincia'] + [f'col_{i}' for i in range(12)]
        
        df_formato_original['provincia'] = df_formato_original['provincia'].str.strip().str.upper()
        df_formato_original['id_provincia_indec'] = df_formato_original['provincia'].map(provincias)

        if df_formato_original['id_provincia_indec'].isnull().any():
            print(f"⚠️ Alerta: No se pudo mapear una o más provincias para el año {anio}.")
        
        df_formato_original = df_formato_original.drop(columns=['provincia'])
        
        fechas = [datetime(int(anio), mes, 1).strftime("%Y-%m-%d") for mes in range(1, 13)]
        df_formato_original.columns = ['id_provincia_indec'] + fechas

        df_melted = df_formato_original.melt(id_vars=['id_provincia_indec'], var_name='fecha', value_name='cantidad')
        df_melted['id_vehiculo'] = tipo_vehiculo

        # *** LÓGICA DE LIMPIEZA MÁS ROBUSTA PARA EVITAR EL ERROR ***
        # 1. Convertimos a string para asegurar que los métodos .str. puedan usarse
        df_melted['cantidad'] = df_melted['cantidad'].astype(str)
        # 2. Eliminamos puntos y comas de la columna 'cantidad'
        df_melted['cantidad'] = df_melted['cantidad'].str.replace(".", "", regex=False)
        df_melted['cantidad'] = df_melted['cantidad'].str.replace(",", "", regex=False)
        # 3. Eliminamos cualquier espacio en blanco
        df_melted['cantidad'] = df_melted['cantidad'].str.strip()
        # 4. Usamos pd.to_numeric con 'coerce' para convertir a número.
        #    Esto convierte los valores que no son números (como 'N/A' o '') a NaN.
        df_melted['cantidad'] = pd.to_numeric(df_melted['cantidad'], errors='coerce').astype('Int64')
        
        # Eliminamos las filas que tienen valores nulos o cero en la cantidad
        df_melted = df_melted.dropna(subset=['cantidad'])
        df_melted = df_melted[df_melted['cantidad'] > 0]
        
        df_list.append(df_melted)

    # Concatenamos todos los DataFrames
    df_total = pd.concat(df_list, ignore_index=True)

    # Conversiones finales y limpieza
    df_total['id_provincia_indec'] = df_total['id_provincia_indec'].astype('Int64')
    df_total['id_vehiculo'] = df_total['id_vehiculo'].astype('Int64')
    df_total['fecha'] = pd.to_datetime(df_total['fecha'], format='%Y-%m-%d')
    
    print("✅ Transformación de datos finalizada.")
    print(df_total.dtypes)
    return df_total
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

        df_formato_original = df_formato_original.iloc[2:26, 0:13].copy()
        
        # Guardamos la primera columna (nombres de provincia) temporalmente
        provincia_nombres = df_formato_original.iloc[:, 0]
        
        # Nos quedamos solo con las columnas de cantidad (los 12 meses)
        df_cantidades = df_formato_original.iloc[:, 1:].copy()
        
        # Creamos el id_provincia_indec a partir de los nombres de provincia
        id_provincia_indec = provincia_nombres.str.strip().str.upper().map(provincias)

        if id_provincia_indec.isnull().any():
            print(f"⚠️ Alerta: No se pudo mapear una o más provincias para el año {anio}. Filas con NaN en id_provincia_indec: {provincia_nombres[id_provincia_indec.isnull()].tolist()}")
        
        # Asignamos id_provincia_indec al DataFrame de cantidades
        df_cantidades['id_provincia_indec'] = id_provincia_indec
        
        # Reordenamos las columnas para que 'id_provincia_indec' sea la primera
        column_order = ['id_provincia_indec'] + [col for col in df_cantidades.columns if col != 'id_provincia_indec']
        df_cantidades = df_cantidades[column_order]

        # Generamos las fechas para los nombres de las columnas
        fechas = [datetime(int(anio), mes, 1).strftime("%Y-%m-%d") for mes in range(1, 13)]
        
        # Asignamos los nombres correctos a las columnas
        # La primera columna es id_provincia_indec, las siguientes 12 son las fechas
        df_cantidades.columns = ['id_provincia_indec'] + fechas

        # Ahora el DataFrame df_cantidades tiene la estructura correcta para el melt
        # con 'id_provincia_indec' y las 'fechas' como columnas
        df_melted = df_cantidades.melt(id_vars=['id_provincia_indec'], var_name='fecha', value_name='cantidad')
        df_melted['id_vehiculo'] = tipo_vehiculo

        # LÓGICA DE LIMPIEZA EXTREMADAMENTE ROBUSTA PARA ELIMINAR EL ERROR '19.281'
        df_melted['cantidad'] = df_melted['cantidad'].astype(str)
        
        def clean_number_string(s):
            if not isinstance(s, str):
                return '' 
            cleaned = ''.join(c for c in s if c.isdigit())
            return cleaned

        df_melted['cantidad'] = df_melted['cantidad'].apply(clean_number_string)
        
        df_melted['cantidad'] = pd.to_numeric(df_melted['cantidad'], errors='coerce').astype(int)
        
        df_melted = df_melted.dropna(subset=['cantidad'])
        df_melted = df_melted[df_melted['cantidad'] > 0]
        
        df_list.append(df_melted)

    df_total = pd.concat(df_list, ignore_index=True)

    df_total['id_provincia_indec'] = df_total['id_provincia_indec'].astype(int)
    df_total['id_vehiculo'] = df_total['id_vehiculo'].astype(int)
    df_total['fecha'] = pd.to_datetime(df_total['fecha'], format='%Y-%m-%d')

    df_total = df_total[['fecha', 'id_provincia_indec', 'id_vehiculo', 'cantidad']]
    
    print("✅ Transformación de datos finalizada.")
    return df_total
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.shared.mappings import aeropuertos

def transform_anac_data(file_path, target_value="TABLA 11"):
    print("🔄 Transformando archivo ANAC.xlsx...")

    try:
        df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        return None

    # Buscar la fila donde se encuentra "TABLA 11"
    fila_target = buscar_fila_por_valor(df, target_value)
    if fila_target is None:
        print(f"⚠️ No se encontró el valor '{target_value}' en el archivo.")
        return None
    
    fila_inicio = fila_target + 2
    df = df.iloc[fila_inicio:(fila_inicio + 58)].copy()
    df.dropna(axis=1, how='all', inplace=True)

    # Transponer
    df = df.transpose()
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    # Fechas desde 2019-01-01
    fecha_inicio = datetime.strptime("2019-01-01", "%Y-%m-%d").date()
    df.insert(0, 'fecha', [fecha_inicio + relativedelta(months=i) for i in range(len(df))])

    # Multiplicar por 1000
    for col in df.columns:
        if col != 'fecha':
            df[col] = pd.to_numeric(df[col], errors='coerce') * 1000
    
    # Renombrar columnas
    df.rename(columns={v: k for k, v in aeropuertos.items()}, inplace=True)

    # Reshape largo
    df_largo = df.melt(id_vars='fecha', var_name='aeropuerto', value_name='cantidad')

    # Limpiar
    df_largo = df_largo.dropna(subset=['cantidad'])

    # Asegurar tipos de datos
    df_largo['fecha'] = pd.to_datetime(df_largo['fecha']).dt.date
    df_largo['aeropuerto'] = df_largo['aeropuerto'].astype(str)
    df_largo['cantidad'] = pd.to_numeric(df_largo['cantidad'], errors='coerce')

    print(df_largo.dtypes)

    print("✅ Transformación completada")
    return df_largo.sort_values(by=['fecha', 'aeropuerto'])

def buscar_fila_por_valor(df, target_value):
    for index, row in df.iterrows():
        if target_value in row.values:
            return index
    return None
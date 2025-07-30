import pandas as pd
import numpy as np

def transform_ripte_data(file_path):
    print("🔄 Transformando archivo RIPTE...")

    try:
        df = pd.read_csv(file_path)
        df = df.replace({np.nan: None})
        df.rename(columns={'indice_tiempo': 'fecha'}, inplace=True)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date

        print("✅ Transformación completada")
        return df
    except Exception as e:
        print(f"❌ Error en la transformación: {e}")
        return None
        
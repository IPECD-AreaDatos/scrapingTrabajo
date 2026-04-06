import pandas as pd
from sqlalchemy import create_engine, text

def find_range_ids_fast():
    engine = create_engine('postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper')
    
    # Obtenemos los últimos 100 periodos registrados (suponiendo que son cronológicos por ID)
    query = text("""
        SELECT DISTINCT codigo_periodo_liquidacion, descripcion_periodo_liquidacion 
        FROM deyc.personal 
        ORDER BY codigo_periodo_liquidacion DESC 
        LIMIT 100
    """)
    
    print("Escaneando los 100 periodos más recientes por ID...")
    try:
        df = pd.read_sql(query, engine)
        print("Mapeo encontrado:")
        pd.set_option('display.max_rows', None)
        print(df.to_string(index=False))
        
        # Filtramos en Pandas los que nos interesan para el WHERE IN
        meses = ["Diciembre", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre"]
        anios = [2024, 2025, 2026]
        
        subset = df[
            (df['descripcion_periodo_liquidacion'].str.contains('|'.join(meses), case=False, na=False)) &
            (df['descripcion_periodo_liquidacion'].str.contains('|'.join(map(str, anios)), case=False, na=False))
        ]
        
        print("\n=== IDs SUGERIDOS PARA LA EXTRACCIÓN (2024-2026) ===")
        print(subset['codigo_periodo_liquidacion'].tolist())
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    find_range_ids_fast()

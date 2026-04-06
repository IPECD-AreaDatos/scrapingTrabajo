import pandas as pd
from sqlalchemy import create_engine, text

def list_recent_periods():
    engine = create_engine('postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper')
    # Usar LIKE para ver los formatos de 2024 y 2026
    query = text("""
        SELECT DISTINCT descripcion_periodo_liquidacion 
        FROM deyc.personal 
        WHERE descripcion_periodo_liquidacion ILIKE '%2024%' 
           OR descripcion_periodo_liquidacion ILIKE '%2026%'
    """)
    print("Obteniendo periodos de 2024 y 2026...")
    try:
        df = pd.read_sql(query, engine)
        print("Periodos encontrados:")
        for p in sorted(df['descripcion_periodo_liquidacion'].tolist()):
            print(f"- {p}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_recent_periods()

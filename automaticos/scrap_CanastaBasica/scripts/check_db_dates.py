import os
import sys
import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    load_dotenv()
    
    host = os.getenv('HOST_DBB2')
    user = os.getenv('USER_DBB2')
    pw = os.getenv('PASSWORD_DBB2')
    db = os.getenv('NAME_DB_CANASTA', 'canasta_basica_super')
    port = os.getenv('PORT_DBB2', '5432')
    
    engine = create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")
    
    q = text("""
        SELECT fecha_extraccion, COUNT(*) as cantidad
        FROM precios_productos 
        WHERE fecha_extraccion >= current_date - interval '14 days'
        GROUP BY fecha_extraccion 
        ORDER BY fecha_extraccion DESC
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
            print("=== CONTEO DE EXTRACCIONES (Últimos 14 días) ===")
            print(df.to_string(index=False))
            
            # Check for missing dates
            if not df.empty:
                min_date = df['fecha_extraccion'].min()
                max_date = df['fecha_extraccion'].max()
                all_dates = pd.date_range(start=min_date, end=max_date).date
                found_dates = pd.to_datetime(df['fecha_extraccion']).dt.date.values
                
                missing_dates = [d.strftime('%Y-%m-%d') for d in all_dates if d not in found_dates]
                
                if missing_dates:
                    print("\n!!! ATENCIÓN: Faltan las siguientes fechas !!!")
                    for d in missing_dates:
                        print(f" - {d}")
                else:
                    print("\nNo hay fechas faltantes en el rango de extracciones.")
    except Exception as e:
        print(f"Error querying DB: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()

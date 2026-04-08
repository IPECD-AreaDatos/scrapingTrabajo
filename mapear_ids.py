import pandas as pd
from sqlalchemy import create_engine, text
import time

def find_range_ids_fast():
    conn_str = 'postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper'
    print("Connecting to 10.1.90.2 (deyc.personal)...", flush=True)
    engine = create_engine(conn_str, connect_args={'connect_timeout': 10})
    
    meses_a_buscar = [
        ('DICIEMBRE 2024', '%DICIEMBRE%2024%'),
        ('ENERO 2025', '%ENERO%2025%'), ('FEBRERO 2025', '%FEBRERO%2025%'), ('MARZO 2025', '%MARZO%2025%'),
        ('ABRIL 2025', '%ABRIL%2025%'), ('MAYO 2025', '%MAYO%2025%'), ('JUNIO 2025', '%JUNIO%2025%'),
        ('JULIO 2025', '%JULIO%2025%'), ('AGOSTO 2025', '%AGOSTO%2025%'), ('SEPTIEMBRE 2025', '%SEPTIEMBRE%2025%'),
        ('OCTUBRE 2025', '%OCTUBRE%2025%'), ('NOVIEMBRE 2025', '%NOVIEMBRE%2025%'), ('DICIEMBRE 2025', '%DICIEMBRE%2025%'),
        ('ENERO 2026', '%ENERO%2026%'), ('FEBRERO 2026', '%FEBRERO%2026%'), ('MARZO 2026', '%MARZO%2026%')
    ]

    print("\nIniciando descubrimiento secuencial de IDs...", flush=True)
    mapping = []
    
    try:
        with engine.connect() as conn:
            for label, pattern in meses_a_buscar:
                start_time = time.time()
                print("Querying structure for one row...", flush=True)
                res = conn.execute(text("SELECT * FROM deyc.personal LIMIT 1")).fetchone()
                print("Row data to see valid values:", res)
                
                print("Finding MAX ID to orient ranges...", flush=True)
                res = conn.execute(text("SELECT MAX(codigo_periodo_liquidacion) FROM deyc.personal")).scalar()
                print(f"REAL MAX ID: {res}")
                return


        print("\n=== RESULTADO DEL MAPEADO ===")
        for mid, mlabel in mapping:
            print(f"ID {mid}: {mlabel}")
            
        print("\nLista de IDs para extract.py:")
        print([m[0] for m in mapping])

    except Exception as e:
        print(f"\nError: {e}", flush=True)

if __name__ == "__main__":
    find_range_ids_fast()

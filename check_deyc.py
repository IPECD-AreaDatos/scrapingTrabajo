import os
from sqlalchemy import create_engine, text

def list_deyc_objects():
    engine = create_engine(
        'postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper',
        connect_args={'connect_timeout': 5}
    )
    with engine.connect() as conn:
        print("Buscando objetos (tablas, vistas) en el esquema deyc...")
        res = conn.execute(text("""
            SELECT c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'deyc';
        """))
        for row in res:
            kind_map = {'r': 'Table', 'v': 'View', 'm': 'Materialized View', 'i': 'Index', 'S': 'Sequence'}
            print(f"Object: {row[0]}, Type: {kind_map.get(row[1], row[1])}")

        # Si deyc.personal es una vista, vamos a ver su definición:
        print("\nDefinicion de la vista deyc.personal (si es vista):")
        try:
            res_def = conn.execute(text("SELECT pg_get_viewdef('deyc.personal', true);"))
            print(res_def.scalar())
        except Exception as e:
            print("No se pudo obtener la definición (posiblemente no es vista):", e)

if __name__ == "__main__":
    list_deyc_objects()

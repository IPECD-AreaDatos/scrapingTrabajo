import os
from sqlalchemy import create_engine, text

def list_tables_pg_class():
    engine = create_engine(
        'postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper',
        connect_args={'connect_timeout': 5, 'options': '-c statement_timeout=5000'}
    )
    with engine.connect() as conn:
        print("Obteniendo tablas desde pg_class...")
        # Ignorar pg_catalog e information_schema
        # nspname = schema, relname = table
        res = conn.execute(text("""
            SELECT n.nspname as schema_name, c.relname as table_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND c.relname ILIKE '%periodo%';
        """))
        for row in res:
            print(f"Schema: {row[0]}, Table: {row[1]}")

        print("Todas las tablas en el esquema deyc:")
        res = conn.execute(text("""
            SELECT n.nspname as schema_name, c.relname as table_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname IN ('deyc', 'public');
        """))
        for row in res:
            print(f"Schema: {row[0]}, Table: {row[1]}")

if __name__ == "__main__":
    list_tables_pg_class()

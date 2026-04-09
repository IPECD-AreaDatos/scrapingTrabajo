from sqlalchemy import create_engine, text

def list_tables():
    engine = create_engine('postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper', connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('deyc', 'public') ORDER BY table_schema, table_name;"))
            for r in res:
                print(r)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()

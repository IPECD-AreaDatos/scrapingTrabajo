from sqlalchemy import create_engine, text
import time

def probe():
    engine = create_engine('postgresql+psycopg2://estadistica:c3ns0$2026@10.1.90.2:5432/sisper', connect_args={'connect_timeout': 5})
    with engine.connect() as conn:
        print("Buscando Max ID real...")
        max_id = conn.execute(text("SELECT MAX(codigo_periodo_liquidacion) FROM deyc.personal")).scalar()
        print(f"Max ID: {max_id}")
        
        # Escaneamos los últimos 50 periodos DISTINTOS pidiendo 1 registro de cada uno
        # Empezamos desde el Max y vamos hacia atras
        # Si un ID no existe, saltamos rápido (index scan)
        print("Mapeando IDs descendentes...")
        for i in range(int(max_id), int(max_id) - 400, -1):
            query = text("SELECT DISTINCT descripcion_periodo_liquidacion FROM deyc.personal WHERE codigo_periodo_liquidacion = :id LIMIT 1")
            res = conn.execute(query, {"id": i}).fetchone()
            if res:
                desc = res[0]
                print(f"ID {i} -> {desc}")
                # Si encontramos Marzo 2026, ya sabemos el tope.
                # Si bajamos hasta Diciembre 2024, ya tenemos el rango.

if __name__ == "__main__":
    probe()

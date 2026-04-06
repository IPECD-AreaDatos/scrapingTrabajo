import pandas as pd
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)

def extract_sisper_range(tanda_size=10000):
    """
    Extrae datos de deyc.personal desde Postgres para el rango Dic 2024 - Mar 2026.
    Retorna un generador de DataFrames.
    """
    ORIGEN = {
        "host": "10.1.90.2",
        "user": "estadistica",
        "pass": "c3ns0$2026",
        "db": "sisper",
        "port": "5432"
    }

    # Motor de origen
    engine_ext = create_engine(
        f"postgresql+psycopg2://{ORIGEN['user']}:{ORIGEN['pass']}@{ORIGEN['host']}:{ORIGEN['port']}/{ORIGEN['db']}",
        connect_args={'connect_timeout': 60}
    )

    # Consulta con rango de periodos (basado en descripción para mayor flexibilidad)
    query = text("""
        SELECT * FROM deyc.personal 
        WHERE (descripcion_periodo_liquidacion ILIKE '%diciembre%2024%')
           OR (descripcion_periodo_liquidacion ILIKE '%2025%')
           OR (descripcion_periodo_liquidacion ILIKE '%enero%2026%')
           OR (descripcion_periodo_liquidacion ILIKE '%febrero%2026%')
           OR (descripcion_periodo_liquidacion ILIKE '%marzo%2026%')
    """)

    logger.info("Iniciando extracción filtrada desde PostgreSQL (10.1.90.2)...")
    
    # Retornamos el procesador por chunks
    return pd.read_sql(query, engine_ext, chunksize=tanda_size)

import pandas as pd
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)

def extract_sisper_range(tanda_size=10000):
    """
    Extrae datos de deyc.personal desde Postgres para el rango Dic 2024 - Mar 2026.
    Camino 1: Utiliza IDs numéricos descubiertos para evitar scans de texto pesados.
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

    # IDs mapeados (Dic 2024 - Mar 2026)
    ids_a_buscar = [
        802, 806, 808, 810, 812, 814, 816, 818, 820, 822, 824, 826, 828, 830, 832, 834, 836, 838, 840, 
        981, 983, 985, 987, 989, 991, 993, 995, 997, 999, 1001, 1003, 1005, 1007, 1009, 1011, 1013, 1015, 
        1017, 1019, 1021, 1023, 1025, 1027, 1029, 1031, 1033, 1035, 1037, 1039, 1041, 1043, 1045, 1047, 1049, 1051, 
        1053, 1055, 1057, 1063, 1065, 1067
    ]

    logger.info(f"Iniciando extracción por IDs ({len(ids_a_buscar)} periodos) desde PostgreSQL...")
    
    for mid in ids_a_buscar:
        logger.info(f"-> Extrayendo datos para ID periodo: {mid}")
        
        query = text("""
            SELECT * FROM deyc.personal 
            WHERE codigo_periodo_liquidacion = :pid
        """)
        
        try:
            # Al usar ID, la respuesta debería ser inmediata
            chunks = pd.read_sql(query, engine_ext, params={"pid": mid}, chunksize=tanda_size)
            
            tanda_count = 0
            for df_chunk in chunks:
                tanda_count += 1
                logger.info(f"   [OK] ID {mid}: Tanda {tanda_count} procesada.")
                yield df_chunk
                
        except Exception as e:
            logger.error(f"   [ERROR] No se pudo extraer ID {mid}: {e}")
            continue # Intentamos el próximo ID para no frenar la descarga completa




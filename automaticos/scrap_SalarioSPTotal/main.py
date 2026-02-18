"""
MAIN - Orquestador ETL para SalarioSPTotal
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractSalarioSPTotal, TransformSalarioSPTotal, LoadSalarioSPTotal
from etl.validate import ValidateSalarioSPTotal
from utils.logger import setup_logger


def main():
    setup_logger("salario_sp_total_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL SALARIO SP TOTAL - %s ===", inicio)

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    pwd  = os.getenv('PASSWORD_DBB')
    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        ruta_sp, ruta_total = ExtractSalarioSPTotal().extract()
        df_sp, df_total = TransformSalarioSPTotal().transform(ruta_sp, ruta_total)
        ValidateSalarioSPTotal().validate(df_sp, df_total)
        loader = LoadSalarioSPTotal(host, user, pwd, db)
        loader.load(df_sp, df_total)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()

"""
VALIDATE - Módulo de validación de datos SRT
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

COLUMNAS_REQUERIDAS = ['fecha', 'id_provincia', 'id_seccion', 'id_grupo', 'id_ciiu',
                       'cant_personas_trabaj_up', 'remuneracion', 'salario']


class ValidateSRT:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] Columnas faltantes: {faltantes}")
        negativos = df[(df['cant_personas_trabaj_up'] < 0) | (df['remuneracion'] < 0)]
        if not negativos.empty:
            logger.warning("[VALIDATE] ATENCIÓN: %d filas con valores negativos.", len(negativos))
        logger.info("[VALIDATE] OK — %d filas, fechas: %s → %s",
                    len(df), df['fecha'].min(), df['fecha'].max())

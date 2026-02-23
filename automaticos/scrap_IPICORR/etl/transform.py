"""
TRANSFORM - Módulo de transformación de datos IPICORR
Responsabilidad: Limpiar y formatear los valores numéricos del IPICORR
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

COLUMNAS_NUMERICAS = [
    'var_ia_nivel_general', 'vim_nivel_general', 'vim_alimentos', 'vim_textil',
    'vim_maderas', 'vim_min_nometalicos', 'vim_metales', 'var_ia_alimentos',
    'var_ia_textil', 'var_ia_maderas', 'var_ia_min_nometalicos', 'var_ia_metales'
]


class TransformIPICORR:
    """Transforma el DataFrame del IPICORR."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formatea los valores numéricos (elimina comas y % y convierte a float).

        Returns:
            pd.DataFrame con valores numéricos limpios
        """
        logger.info("[TRANSFORM] Formateando %d filas del IPICORR...", len(df))
        df = df.copy()
        for col in COLUMNAS_NUMERICAS:
            if col in df.columns:
                df[col] = df[col].apply(self._format_value)
        logger.info("[TRANSFORM] IPICORR transformado correctamente.")
        return df

    @staticmethod
    def _format_value(value) -> float:
        return (float(str(value).replace(',', '').replace('%', '')) / 10) / 100

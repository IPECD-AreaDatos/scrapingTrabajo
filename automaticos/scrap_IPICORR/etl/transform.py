"""
TRANSFORM - Módulo de transformación de datos IPICORR
Responsabilidad: Limpiar y formatear los valores numéricos del IPICORR
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

COLUMNAS_NUMERICAS = [
    'Var_ia_Nivel_General', 'Vim_Nivel_General', 'Vim_Alimentos', 'Vim_Textil',
    'Vim_Maderas', 'Vim_min_nometalicos', 'Vim_metales', 'Var_ia_Alimentos',
    'Var_ia_Textil', 'Var_ia_Maderas', 'Var_ia_min_nometalicos', 'Var_ia_metales'
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

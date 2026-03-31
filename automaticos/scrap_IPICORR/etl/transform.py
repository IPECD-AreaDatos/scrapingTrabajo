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
        """
        Convierte valores de Google Sheets a float, manejando errores de formato
        y textos no numéricos como 'n.a'.
        """
        try:
            # 1. Limpieza básica: pasar a string, quitar espacios y minúsculas
            val_str = str(value).strip().lower()

            # 2. Manejo de casos conocidos de datos faltantes
            if val_str in ['n.a', 'na', '', 'nan', 'none', '-', 'null']:
                return 0.0  # Retorna 0.0 para evitar errores en cálculos posteriores

            # 3. Limpieza de símbolos y conversión
            # Reemplazamos comas y porcentajes para poder convertir a float
            clean_val = val_str.replace(',', '').replace('%', '')
            
            return (float(clean_val) / 10) / 100

        except (ValueError, TypeError):
            # Si algo falla (ej: un texto largo inesperado), devuelve 0.0 y no rompe el script
            return 0.0
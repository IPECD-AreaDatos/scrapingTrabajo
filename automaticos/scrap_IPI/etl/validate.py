"""
VALIDATE - Módulo de validación de datos IPI
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPI:
    def validate(self, df: pd.DataFrame):
        """
        Realiza validaciones estructurales y de contenido en el DataFrame unificado.
        """
        # 1. Verificar si el DataFrame existe y tiene datos
        if df is None or df.empty:
            raise ValueError("[VALIDATE] El DataFrame unificado del IPI está vacío.")

        # 2. Definir columnas críticas que DEBEN existir después del merge
        # Usamos nombres en minúsculas ya que el Transform los normaliza
        cols_criticas = [
            'fecha', 
            'ipi_manufacturero', 
            'var_ipi', 
            'ipi_manufacturero_inter_acum'
        ]

        faltantes = [c for c in cols_criticas if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] Faltan columnas esenciales tras la unificación: {faltantes}")

        # 3. Validar integridad de fechas
        if df['fecha'].isnull().any():
            raise ValueError("[VALIDATE] Se detectaron valores nulos en la columna 'fecha'.")

        # 4. Validar que no haya valores extremos incoherentes (opcional pero recomendado)
        # Por ejemplo, el IPI manufacturero no debería ser negativo
        if (df['ipi_manufacturero'] < 0).any():
            logger.warning("[VALIDATE] Se detectaron valores negativos en 'ipi_manufacturero'. Revisar origen.")

        logger.info(f"[VALIDATE] OK — DataFrame unificado válido: {len(df)} filas.")

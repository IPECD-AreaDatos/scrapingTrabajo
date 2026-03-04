"""
VALIDATE - Módulo de validación de datos IPI
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ValidateIPI:
    def validate(self, datos_ipi: dict):
        """Valida el diccionario de DataFrames del IPI."""
        
        # Validar IPI (Valores y variaciones mensuales)
        cols_ipi = ['fecha', 'ipi_manufacturero', 'alimentos', 'textil', 'maderas', 
                    'sustancias', 'min_no_metalicos', 'min_metales', 
                    'var_mensual_ipi_manufacturero', 'var_mensual_alimentos', 
                    'var_mensual_textil', 'var_mensual_maderas', 
                    'var_mensual_sustancias', 'var_mensual_min_no_metalicos', 
                    'var_mensual_min_metales']
        self._validar_df(datos_ipi['valores'], "ipi", cols_ipi)
        
        # Validar IPI Variación Interanual
        cols_var = ['fecha', 'var_ipi', 'var_interanual_alimentos', 'var_interanual_textil', 
                    'var_interanual_sustancias', 'var_interanual_maderas', 
                    'var_interanual_min_no_metalicos', 'var_interanual_metales'] # <-- Agregué guiones bajos
        self._validar_df(datos_ipi['variaciones'], "ipi_variacion_interanual", cols_var)
        
        # Validar IPI Variación Interacumulada
        cols_acu = [
            'fecha', 
            'ipi_manufacturero_inter_acum', 
            'alimentos_inter_acum', 
            'textil_inter_acum', 
            'maderas_inter_acum', 
            'sustancias_inter_acum', 
            'min_no_metalicos_inter_acum', 
            'metales_inter_acum'
        ]
        self._validar_df(datos_ipi['acumulado'], "ipi_variacion_interacumulada", cols_acu)

        logger.info("[VALIDATE] OK — Todas las tablas del IPI son válidas.")

    def _validar_df(self, df: pd.DataFrame, nombre: str, cols_minimas: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] La tabla '{nombre}' está vacía.")
        
        # Validar existencia de columnas
        faltantes = [c for c in cols_minimas if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' faltan columnas: {faltantes}")
            
        if df['fecha'].isnull().any():
            raise ValueError(f"[VALIDATE] Valores nulos en 'fecha' de '{nombre}'.")
            
        logger.info(f"[VALIDATE] '{nombre}' OK — {len(df)} filas.")
"""
Módulo de Transformación para Canasta Básica
Responsabilidad: Consolidar, limpiar y normalizar datos extraídos
"""
import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class TransformCanastaBasica:
    """Clase para manejar la transformación de datos de Canasta Básica"""
    
    def __init__(self):
        pass
    
    def transform(self, all_supermarkets_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Consolida y transforma datos de todos los supermercados"""
        logger.info("[TRANSFORM] Iniciando transformación de datos...")
        
        if not all_supermarkets_data:
            logger.warning("[WARNING] No hay datos para transformar")
            return pd.DataFrame()
        
        # Consolidar todos los DataFrames
        all_dataframes = list(all_supermarkets_data.values())
        consolidated_df = pd.concat(all_dataframes, ignore_index=True)
        
        logger.info("[TRANSFORM] Datos consolidados: %d registros de %d supermercados", 
                len(consolidated_df), len(all_supermarkets_data))
        
        # Aplicar transformaciones
        consolidated_df = self._clean_columns(consolidated_df)
        consolidated_df = self._sort_data(consolidated_df)
        consolidated_df = self._reorder_columns(consolidated_df)
        
        logger.info("[OK] DataFrame transformado: %d filas, %d columnas", 
                len(consolidated_df), len(consolidated_df.columns))
        
        return consolidated_df
    
    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia columnas duplicadas y normaliza nombres"""
        # Eliminar columna 'unidad' si existe 'unidad_medida' (duplicada)
        if 'unidad' in df.columns and 'unidad_medida' in df.columns:
            logger.info("[TRANSFORM] Eliminando columna duplicada 'unidad', conservando 'unidad_medida'")
            df = df.drop(columns=['unidad'])
        
        return df
    
    def _sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ordena los datos por producto y supermercado"""
        if 'producto_nombre' in df.columns and 'supermercado' in df.columns:
            df = df.sort_values(['producto_nombre', 'supermercado'])
        
        return df
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reordena columnas para mejor presentación"""
        column_order = [
            'producto_nombre', 'nombre', 'supermercado', 
            'precio_normal', 'precio_descuento', 'precio_por_unidad',
            'unidad_medida', 'peso', 'descuentos',
            'fecha', 'fecha_extraccion', 'url'
        ]
        
        # Agregar columnas de error si existen
        if 'error_type' in df.columns:
            column_order.append('error_type')
        
        # Reordenar columnas existentes
        existing_columns = [col for col in column_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in column_order]
        
        return df[existing_columns + other_columns]










"""
Core del sistema ETL - Clases base y componentes fundamentales
"""
from core.base_extractor import BaseExtractor
from core.base_transformer import BaseTransformer
from core.base_loader import BaseLoader
from core.pipeline_runner import PipelineRunner

__all__ = [
    'BaseExtractor',
    'BaseTransformer',
    'BaseLoader',
    'PipelineRunner',
]




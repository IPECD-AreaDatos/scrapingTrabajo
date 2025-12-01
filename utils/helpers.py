"""
Funciones helper generales
"""
from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path
import json


def ensure_dir(path: Path) -> Path:
    """
    Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        path: Ruta del directorio
        
    Returns:
        Path del directorio
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_json(data: Dict[str, Any], file_path: Path) -> None:
    """
    Guarda un diccionario como archivo JSON.
    
    Args:
        data: Datos a guardar
        file_path: Ruta del archivo
    """
    ensure_dir(file_path.parent)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Carga un archivo JSON.
    
    Args:
        file_path: Ruta del archivo
        
    Returns:
        Diccionario con los datos o None si no existe
    """
    if not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Realiza una división segura evitando división por cero.
    
    Args:
        numerator: Numerador
        denominator: Denominador
        default: Valor por defecto si el denominador es cero
        
    Returns:
        Resultado de la división o valor por defecto
    """
    if denominator == 0:
        return default
    return numerator / denominator


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Divide una lista en chunks de tamaño específico.
    
    Args:
        lst: Lista a dividir
        chunk_size: Tamaño de cada chunk
        
    Returns:
        Lista de chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def clean_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia los nombres de columnas de un DataFrame.
    - Convierte a minúsculas
    - Reemplaza espacios por guiones bajos
    - Elimina caracteres especiales
    
    Args:
        df: DataFrame a limpiar
        
    Returns:
        DataFrame con columnas limpiadas
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('[^a-z0-9_]', '', regex=True)
    )
    return df




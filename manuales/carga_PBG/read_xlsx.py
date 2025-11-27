# read_xlsx.py
from __future__ import annotations
import pandas as pd

def list_sheets(file_path: str) -> list[str]:
    """Devuelve la lista de hojas disponibles en el .xlsx."""
    try:
        return pd.ExcelFile(file_path, engine="openpyxl").sheet_names
    except FileNotFoundError:
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    except Exception as e:
        raise RuntimeError(f"Error al abrir el Excel: {e}")

def read_excel(file_path: str, sheet_name: str | int, **read_kwargs) -> pd.DataFrame:
    """
    Lee una hoja específica del Excel y devuelve un DataFrame.
    Puedes pasar kwargs típicos de pandas.read_excel (skiprows, dtype, usecols, etc.).
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl", **read_kwargs)
        # Limpiezas suaves:
        # - recorta espacios en nombres de columnas
        df.columns = [str(c).strip() for c in df.columns]
        # - opcional: elimina filas totalmente vacías
        df = df.dropna(how="all")
        return df
    except ValueError as e:
        # Suele indicar que la hoja no existe
        raise ValueError(f"No se pudo leer la hoja '{sheet_name}': {e}")
    except Exception as e:
        raise RuntimeError(f"Error al leer '{sheet_name}': {e}")

def read_sheets(file_path: str, sheet_names: list[str] | tuple[str, ...], **read_kwargs) -> dict[str, pd.DataFrame]:
    """
    Lee múltiples hojas y devuelve un dict {nombre_hoja: DataFrame}.
    Propaga kwargs a read_excel.
    """
    result: dict[str, pd.DataFrame] = {}
    available = set(list_sheets(file_path))
    missing = [s for s in sheet_names if s not in available]
    if missing:
        raise ValueError(
            f"Las hojas requeridas no existen en el archivo: {missing}. "
            f"Hojas disponibles: {sorted(available)}"
        )
    for s in sheet_names:
        result[s] = read_excel(file_path, s, **read_kwargs)
    return result

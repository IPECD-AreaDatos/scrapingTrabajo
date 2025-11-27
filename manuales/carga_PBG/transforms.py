# transforms.py
from __future__ import annotations
import pandas as pd
import re

_ROMAN_Q = re.compile(r"^\s*(\d{4})\s*-\s*(I{1,3}|IV)\s*$")  # Año - I|II|III|IV
_TR_MAP = {"I": 1, "II": 2, "III": 3, "IV": 4}

def _split_periodo(periodo: str) -> tuple[str | None, str | None]:
    """Devuelve (Año, Trimestre) a partir de 'YYYY - I|II|III|IV'. Si no matchea, (None, None)."""
    if pd.isna(periodo):
        return (None, None)
    m = _ROMAN_Q.match(str(periodo))
    if not m:
        return (None, None)
    return (m.group(1), m.group(2).strip())

def transform_trimestral(df: pd.DataFrame, group_keys=("Actividad",)) -> pd.DataFrame:
    """
    Normaliza y calcula variación vs trimestre anterior:
      - 'Periodo' -> 'Año' (string) y 'Trimestre' (I..IV)
      - Limpia 'Valor'
      - Ordena por secuencia trimestral correcta
      - Calcula VariacionT (%) por grupos (p.ej., por Actividad)
      - Devuelve: Año, Trimestre, Variable, Actividad, Valor, VariacionT
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Año / Trimestre
    anos, tris = zip(*df["Periodo"].map(_split_periodo))
    df["Año"] = pd.Series(anos, dtype="string")
    df["Trimestre"] = pd.Series(tris, dtype="string")

    # Texto limpio
    for col in ["Variable", "Actividad"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Valor numérico (maneja coma/punto)
    if df["Valor"].dtype == "object":
        df["Valor"] = (
            df["Valor"].astype("string")
            .str.replace(".", "", regex=False)    # miles
            .str.replace(",", ".", regex=False)   # coma -> punto
        )
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    # Filtrar inválidos
    df = df.dropna(subset=["Año", "Trimestre", "Valor"]).reset_index(drop=True)

    # Orden trimestral global (años pueden cruzar IV -> I del siguiente año)
    df["Año_num"] = df["Año"].astype("int64")
    df["Tri_num"] = df["Trimestre"].map(_TR_MAP).astype("int64")
    df["t_index"] = df["Año_num"] * 4 + df["Tri_num"]  # 2004-I < 2004-II < ... < 2005-I

    # Agrupar (por defecto solo por Actividad; podés pasar ("Actividad","Variable"))
    gcols = list(group_keys)

    df = df.sort_values(gcols + ["t_index"])
    df["Variacion"] = df.groupby(gcols)["Valor"].pct_change() * 100

    # Selección y orden final
    out_cols = ["Año", "Trimestre", "Variable", "Actividad", "Valor", "Variacion"]
    df = df[out_cols].reset_index(drop=True)

    return df

def _coerce_to_year(s: pd.Series) -> pd.Series:
    """
    Normaliza el año desde strings/nums con posibles espacios o formatos raros.
    Devuelve string (para alinear con tu esquema SQL donde Año es TEXT).
    """
    s = s.astype("string").str.strip()
    # Reemplazos suaves: 2004.0 -> 2004 ; "2004-01" -> "2004"
    s = s.str.replace(r"[^\d]", "", regex=True)
    # Si queda vacío -> <NA>
    s = s.mask(s.eq(""))
    return s

# transforms.py (agregamos al final)

def transform_anual(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza la hoja anual:
      - Usa Año desde 'Periodo' o 'Año'
      - Elimina 'Frecuencia'
      - Convierte columnas a tipos correctos
      - Calcula Variación (%) por Actividad entre años consecutivos
      - Devuelve columnas: Año, Variable, Actividad, Valor, Variacion (%)
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Año
    year_col = "Periodo" if "Periodo" in df.columns else "Año"
    df["Año"] = _coerce_to_year(df[year_col])

    # Texto
    for col in ["Año", "Variable", "Actividad"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Valor numérico
    if "Valor" not in df.columns:
        raise ValueError("No se encontró la columna 'Valor' en la hoja Anual.")

    if df["Valor"].dtype == "object":
        df["Valor"] = (
            df["Valor"].astype("string")
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    # Filtrar inválidos
    df = df.dropna(subset=["Año", "Valor"]).reset_index(drop=True)

    # Ordenar por Actividad y Año (numérico)
    df["Año_num"] = df["Año"].astype("int64")
    df = df.sort_values(by=["Actividad", "Año_num"])

    # dentro de transform_anual
    df["Variacion"] = df.groupby("Actividad")["Valor"].pct_change() * 100

    wanted = ["Año", "Variable", "Actividad", "Valor", "Variacion"]
    df = df[wanted].reset_index(drop=True)

    return df

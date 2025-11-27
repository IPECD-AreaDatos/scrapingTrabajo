# loaders.py
from __future__ import annotations
import pandas as pd
from sqlalchemy import text

def _chunks(lst, size=1000):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _round2(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    return df

def truncate_trimestral(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE pbg_valor_trimestral"))

def truncate_anual(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE pbg_valor_anual"))

def insert_trimestral(df: pd.DataFrame, engine) -> None:
    """
    Inserta en pbg_valor_trimestral:
    Año, Trimestre, Variable, Actividad, Valor, Variacion
    (se asume TRUNCATE ya ejecutado si se desea recargar desde cero)
    """
    df = df[["Año", "Trimestre", "Variable", "Actividad", "Valor", "Variacion"]].copy()
    df = _round2(df, ["Valor", "Variacion"])  # ✅ 2 decimales

    # Preparamos registros (evitamos tildes en nombres de parámetros)
    records = [{
        "anio":       str(row["Año"]) if pd.notna(row["Año"]) else None,
        "trimestre":  (row["Trimestre"] if pd.notna(row["Trimestre"]) else None),
        "variable":   (row["Variable"] if pd.notna(row["Variable"]) else None),
        "actividad":  (row["Actividad"] if pd.notna(row["Actividad"]) else None),
        "valor":      (row["Valor"] if pd.notna(row["Valor"]) else None),
        "variacion": (row["Variacion"] if pd.notna(row["Variacion"]) else None),
    } for _, row in df.iterrows()]

    # Además de venir redondeado, forzamos ROUND(...,2) en SQL por robustez
    sql = text("""
        INSERT INTO pbg_valor_trimestral
            (`Año`, `Trimestre`, `Variable`, `Actividad`, `Valor`, `Variacion`)
        VALUES
            (:anio, :trimestre, :variable, :actividad, ROUND(:valor,2), ROUND(:variacion,2))
    """)

    with engine.begin() as conn:
        for batch in _chunks(records, 2000):
            conn.execute(sql, batch)

def insert_anual(df: pd.DataFrame, engine) -> None:
    """
    Inserta en pbg_valor_anual:
    Año, Variable, Actividad, Valor, Variacion
    (se asume TRUNCATE ya ejecutado si se desea recargar desde cero)
    """
    df = df[["Año", "Variable", "Actividad", "Valor", "Variacion"]].copy()
    df = _round2(df, ["Valor", "Variacion"])  # ✅ 2 decimales

    records = [{
        "anio":      str(row["Año"]) if pd.notna(row["Año"]) else None,
        "variable":  (row["Variable"] if pd.notna(row["Variable"]) else None),
        "actividad": (row["Actividad"] if pd.notna(row["Actividad"]) else None),
        "valor":     (row["Valor"] if pd.notna(row["Valor"]) else None),
        "variacion": (row["Variacion"] if pd.notna(row["Variacion"]) else None),
    } for _, row in df.iterrows()]

    sql = text("""
        INSERT INTO pbg_valor_anual
            (`Año`, `Variable`, `Actividad`, `Valor`, `Variacion`)
        VALUES
            (:anio, :variable, :actividad, ROUND(:valor,2), ROUND(:variacion,2))
    """)

    with engine.begin() as conn:
        for batch in _chunks(records, 2000):
            conn.execute(sql, batch)

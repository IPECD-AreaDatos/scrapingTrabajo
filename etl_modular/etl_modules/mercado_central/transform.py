import pandas as pd
from pathlib import Path
import re
import warnings

warnings.simplefilter("ignore", category=UserWarning)

def procesar_archivos_csv(base_path, patron_nombre, rubro=None):
    data = []

    for archivo in Path(base_path).rglob("*.csv"):
        try:
            # Leer y estandarizar nombres
            df = pd.read_csv(archivo)
            df.columns = df.columns.str.strip().str.upper()

            # Filtrar columnas deseadas
            columnas_base = ["ESP", "VAR", "PROC", "ENV", "KG", "CAL", "TAM", "GRADO"]
            columnas_precios = [col for col in df.columns if re.match(r"^(MA|MO|MI)\d{6}$", col)] + ["MAPK", "MOPK", "MIPK"]
            columnas_utiles = columnas_base + columnas_precios
            df = df[[col for col in columnas_utiles if col in df.columns]]

            # Normalizar PROC
            if "PROC" not in df.columns:
                raise ValueError("No se encontró la columna 'PROC'")

            df["PROC"] = (
                df["PROC"]
                .astype(str)
                .str.upper()
                .str.replace(".", "", regex=False)
                .str.replace("Á", "A").str.replace("É", "E")
                .str.replace("Í", "I").str.replace("Ó", "O").str.replace("Ú", "U")
                .str.strip()
            )


            df = df[df["PROC"].isin(["CTES", "CORRIENTES"])]
            if df.empty:
                continue

            # Fecha desde nombre de archivo
            match = re.search(patron_nombre, archivo.name.upper())
            if match:
                dia, mes, anio = match.groups()
                fecha = f"20{anio}-{mes}-{dia}"
                df["FECHA"] = pd.to_datetime(fecha)
            else:
                raise ValueError("Nombre de archivo no contiene fecha válida")

            if rubro:
                df["RUBRO"] = rubro

            data.append(df)

        except Exception as e:
            print(f"⚠️ Error en {archivo.name}: {e}")

    return data


def transform_mercado_central_completo():
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    frutas_path = BASE_DIR / "data" / "raw" / "mercado_central" / "frutas2024"
    hortalizas_path = BASE_DIR / "data" / "raw" / "mercado_central" / "hortalizas2024"

    frutas = procesar_archivos_csv(
        base_path=frutas_path,
        patron_nombre=r"RF(\d{2})(\d{2})(\d{2})",
        rubro="fruta"
    )

    hortalizas = procesar_archivos_csv(
        base_path=hortalizas_path,
        patron_nombre=r"RH(\d{2})(\d{2})(\d{2})",
        rubro="hortaliza"
    )

    df_frutas = pd.concat(frutas, ignore_index=True) if frutas else pd.DataFrame()
    df_hortalizas = pd.concat(hortalizas, ignore_index=True) if hortalizas else pd.DataFrame()

    print(f"✅ Frutas: {len(df_frutas)} registros")
    print(f"✅ Hortalizas: {len(df_hortalizas)} registros")

    if df_frutas.empty and df_hortalizas.empty:
        raise ValueError("❌ No se encontraron datos PROC=CTES válidos")

    # ✅ Columnas específicas para frutas y hortalizas
    columnas_finales_frutas = [
        "ESP", "VAR", "PROC", "ENV", "KG", "CAL", "TAM", "GRADO",
        "MA021224", "MO021224", "MI021224", "MAPK", "MOPK", "MIPK",
        "FECHA", "RUBRO"
    ]

    columnas_finales_hortalizas = [
        "ESP", "VAR", "PROC", "ENV", "KG", "CAL", "TAM", "GRADO",
        "MA010824", "MO010824", "MI010824", "MAPK", "MOPK", "MIPK",
        "FECHA", "RUBRO"
    ]

    df_frutas = df_frutas[[col for col in columnas_finales_frutas if col in df_frutas.columns]]
    df_hortalizas = df_hortalizas[[col for col in columnas_finales_hortalizas if col in df_hortalizas.columns]]
    print("🍎 Columnas en df_frutas ANTES:")
    print(df_frutas.columns.tolist())

    print("\n🥕 Columnas en df_hortalizas ANTES:")
    print(df_hortalizas.columns.tolist())

    # ✅ Renombrar columnas y pasar a minúsculas
    df_frutas = renombrar_columnas(df_frutas)
    df_hortalizas = renombrar_columnas(df_hortalizas)

    print("🍎 Columnas en df_frutas DPS:")
    print(df_frutas.columns.tolist())

    print("\n🥕 Columnas en df_hortalizas DPS:")
    print(df_hortalizas.columns.tolist())

    return df_frutas, df_hortalizas

def renombrar_columnas(df):
    nuevos_nombres = {}
    for col in df.columns:
        if col.startswith("MA") and not col.endswith("PK"):
            nuevos_nombres[col] = "maximo"
        elif col.startswith("MO") and not col.endswith("PK"):
            nuevos_nombres[col] = "moda"
        elif col.startswith("MI") and not col.endswith("PK"):
            nuevos_nombres[col] = "minimo"
    df = df.rename(columns=nuevos_nombres)
    df.columns = df.columns.str.lower()  # pasar todas a minúsculas
    return df
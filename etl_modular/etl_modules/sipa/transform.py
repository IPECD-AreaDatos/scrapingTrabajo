from etl_modular.utils.helpers import limpiar_nombre
from etl_modular.utils.mappings import provincias, categorias_nacion_sipa
import pandas as pd
import numpy as np


def transform_sipa_data(file_path):
    print("🔄 Transformando archivo SIPA.xlsx...")

    def leer_excel(path, sheet, skip):
        df = pd.read_excel(path, sheet_name=sheet, skiprows=skip)
        df = df.replace({np.nan: None, ",": "."}, regex=True).infer_objects()
        df = df[~df.iloc[:, 0].astype(str).str.contains("nota", case=False, na=False)]
        df = df[df.iloc[:, 1:].notnull().any(axis=1)]
        df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
        return df

    df_e_prov = leer_excel(file_path, sheet=13, skip=1)
    df_ne_prov = leer_excel(file_path, sheet=14, skip=1)
    df_e_nac = leer_excel(file_path, sheet=3, skip=1)
    df_ne_nac = leer_excel(file_path, sheet=4, skip=1)

    df_e_prov["Período"] = pd.date_range(
        start="2009-01-01", periods=len(df_e_prov), freq="MS"
    )
    df_ne_prov["Período"] = pd.date_range(
        start="2009-01-01", periods=len(df_ne_prov), freq="MS"
    )
    df_e_nac["Período"] = pd.date_range(
        start="2012-01-01", periods=len(df_e_nac), freq="MS"
    )
    df_ne_nac["Período"] = pd.date_range(
        start="2012-01-01", periods=len(df_ne_nac), freq="MS"
    )

    registros = []

    # 👉 Armamos mapeo col_limpia → col_original
    columnas_limpias = {
        limpiar_nombre(col): col for col in df_e_prov.columns if col != "Período"
    }
    
    # Provincias
    for (_, row_e), (_, row_ne) in zip(df_e_prov.iterrows(), df_ne_prov.iterrows()):
        fecha = row_e["Período"]
        for col_limpia, col_original in columnas_limpias.items():
            if col_limpia == "UNNAMED":
                continue  # 🛑 Ignorar columnas basura
            id_prov = provincias.get(col_limpia)
            if id_prov:
                registros.append(
                    {
                        "fecha": fecha,
                        "id_provincia": id_prov,
                        "id_tipo_registro": 1,
                        "cantidad_con_estacionalidad": row_e.get(col_original),
                        "cantidad_sin_estacionalidad": row_ne.get(col_original),
                    }
                )
            else:
                print(f"⚠️ Provincia no reconocida: {col_limpia}")

    # Nación
    for (_, row_e), (_, row_ne) in zip(df_e_nac.iterrows(), df_ne_nac.iterrows()):
        fecha = row_e["Período"]
        for cat, id_registro in categorias_nacion_sipa.items():
            registros.append(
                {
                    "fecha": fecha,
                    "id_provincia": 1,
                    "id_tipo_registro": id_registro,
                    "cantidad_con_estacionalidad": row_e.get(cat),
                    "cantidad_sin_estacionalidad": row_ne.get(cat),
                }
            )

    df_final = pd.DataFrame(registros)
    print("✅ Transformación completada")
    return df_final.sort_values(by=["fecha", "id_provincia", "id_tipo_registro"])

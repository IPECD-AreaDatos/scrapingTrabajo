from src.shared.helpers import limpiar_nombree, contar_meses_validos
from src.shared.mappings import provincias_superm
import pandas as pd
import numpy as np


def transform_sipa_data(file_path):

    def leer_excel(path, sheet, skip):
        df = pd.read_excel(path, sheet_name=sheet, skiprows=skip)
        df = df.replace({np.nan: None, ",": "."}, regex=True).infer_objects()
        df = df[~df.iloc[:, 0].astype(str).str.contains("nota", case=False, na=False)]
        df = df[df.iloc[:, 1:].notnull().any(axis=1)]
        df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
        return df

    df_e_prov = leer_excel(file_path, sheet=13, skip=1) #Hoja A.5.1 personas con empleo asalariado registado en el sector privado segun provincia. CON ESTACIONALIDAD
    df_ne_prov = leer_excel(file_path, sheet=15, skip=1) #Hoja A.5.2 personas con empleo asalariado registado en el sector privado segun provincia. SIN ESTACIONALIDAD
    df_e_nac = leer_excel(file_path, sheet=3, skip=1) #Hoja T.2.1 personas con trabajo registrado con estacionalidad total pais
    df_ne_nac = leer_excel(file_path, sheet=4, skip=1) #Hoja T.2.2 personas con trabajo registrado sin estacionalidad total pais

    
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

    # Crear mapeos de columnas limpias → nombre original para ambos DataFrames
    cols_e = {
        limpiar_nombre(col): col
        for col in df_e_prov.columns
        if col != "Período" and not "Unnamed" in col
    }

    cols_ne = {
        limpiar_nombre(col): col
        for col in df_ne_prov.columns
        if col != "Período" and not "Unnamed" in col
    }

    # Hacer intersección de columnas limpias (las que están en ambos)
    columnas_limpias = {
        col_limpia: (cols_e[col_limpia], cols_ne[col_limpia])
        for col_limpia in (set(cols_e) & set(cols_ne))
    }


    # Provincias
    for (_, row_e), (_, row_ne) in zip(df_e_prov.iterrows(), df_ne_prov.iterrows()):
        fecha = row_e["Período"]
        for col_limpia, (col_e, col_ne) in columnas_limpias.items():
            id_prov = provincias.get(col_limpia)
            if id_prov:
                registros.append(
                    {
                        "fecha": fecha,
                        "id_provincia": id_prov,
                        "id_tipo_registro": 1,
                        "cantidad_con_estacionalidad": row_e.get(col_e),
                        "cantidad_sin_estacionalidad": row_ne.get(col_ne),
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
    return df_final.sort_values(by=["fecha", "id_provincia", "id_tipo_registro"])

import os
import pandas as pd

codigo_provincias = {
    "Nacion": 1,
    "Ciudad De Buenos Aires": 2,
    "Buenos Aires": 6,
    "Catamarca": 10,
    "Cordoba": 14,
    "Corrientes": 18,
    "Chaco": 22,
    "Chubut": 26,
    "Entre Rios": 30,
    "Formosa": 34,
    "Jujuy": 38,
    "La Pampa": 42,
    "La Rioja": 46,
    "Mendoza": 50,
    "Misiones": 54,
    "Neuquen": 58,
    "Rio Negro": 62,
    "Salta": 66,
    "San Juan": 70,
    "San Luis": 74,
    "Santa Cruz": 78,
    "Santa Fe": 82,
    "Santiago Del Estero": 86,
    "Tucuman": 90,
    "Tierra Del Fuego": 94
}

def normalize_province_name(folder_name):
    return folder_name.replace("_", " ").title()

class readFileOcupacion:
    def __init__(self):
        self.total_pais_cargado = False

    def read_file(self):
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        dfs = []

        for provincia_folder in os.listdir(base_dir):
            path_file = os.path.join(base_dir, provincia_folder, 'puestos_de_trabajo_registrados.xls')
            if not os.path.exists(path_file):
                continue

            provincia_normalizada = normalize_province_name(provincia_folder)
            id_provincia = codigo_provincias.get(provincia_normalizada)

            if not id_provincia:
                print(f"⚠️ Provincia no reconocida: {provincia_normalizada}")
                continue

            try:
                df = pd.read_excel(path_file, skiprows=4)
                df = df.dropna(subset=[df.columns[0]])

                # Detectar si es Buenos Aires
                if provincia_normalizada == "Buenos Aires":
                    col_mensual = 5
                    col_interanual = 6
                    col_acumulada = 7
                else:
                    col_mensual = 3
                    col_interanual = 4
                    col_acumulada = 5

                df_prov = pd.DataFrame()
                df_prov['fecha'] = pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date
                df_prov['puestos_de_trabajo'] = pd.to_numeric(df.iloc[:, 1], errors='coerce')
                df_prov['porcentaje_var_mensual'] = (pd.to_numeric(df.iloc[:, col_mensual], errors='coerce') / 100).round(5)
                df_prov['porcentaje_var_interanual'] = (pd.to_numeric(df.iloc[:, col_interanual], errors='coerce') / 100).round(5)
                df_prov['porcentaje_var_acumulada'] = (pd.to_numeric(df.iloc[:, col_acumulada], errors='coerce') / 100).round(5)
                df_prov['id_provincia'] = id_provincia
                dfs.append(df_prov)

                if not self.total_pais_cargado and df.shape[1] >= 11:
                    try:
                        df_total = pd.DataFrame()
                        df_total['fecha'] = pd.to_datetime(df.iloc[:, 0], errors='coerce').dt.date
                        df_total['puestos_de_trabajo'] = pd.to_numeric(df.iloc[:, 4], errors='coerce')
                        df_total['porcentaje_var_mensual'] = (pd.to_numeric(df.iloc[:, 8], errors='coerce') / 100).round(5)
                        df_total['porcentaje_var_interanual'] = (pd.to_numeric(df.iloc[:, 9], errors='coerce') / 100).round(5)
                        df_total['porcentaje_var_acumulada'] = (pd.to_numeric(df.iloc[:, 10], errors='coerce') / 100).round(5)
                        df_total['id_provincia'] = 1
                        dfs.append(df_total)
                        self.total_pais_cargado = True
                        print(f"✅ Nación cargada desde: {provincia_normalizada}")
                    except Exception as e:
                        print(f"⚠️ Fallo al extraer Nación desde {provincia_normalizada}: {e}")

            except Exception as e:
                print(f"❌ Error leyendo {path_file}: {e}")

        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final[['fecha', 'id_provincia', 'puestos_de_trabajo',
                             'porcentaje_var_mensual', 'porcentaje_var_interanual', 'porcentaje_var_acumulada']]
        df_final = df_final.dropna(subset=['fecha'])

        return df_final

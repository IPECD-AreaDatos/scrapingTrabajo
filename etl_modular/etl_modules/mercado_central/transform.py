import pandas as pd
from pathlib import Path
import re
import warnings

warnings.simplefilter(action='ignore', category=UserWarning)  # Ignorar warnings de read_html

def transform_mercado_central_data():
    base_path = Path("data/raw/mercado_central/frutas2024/frutas2024")
    data = []

    proc_validos = ["RF", "RB", "RH", "RP"]

    for carpeta_mes in base_path.iterdir():
        if carpeta_mes.is_dir():
            for archivo in carpeta_mes.glob("*.XLS"):
                try:
                    # Intentar leer como archivo Excel (formato verdadero .xls)
                    try:
                        df = pd.read_excel(archivo, header=0, engine="xlrd")
                    except Exception:
                        # Si falla, intentar leer como archivo HTML renombrado
                        df_list = pd.read_html(archivo)
                        df = df_list[0]  # Tomar la primera tabla

                    # Estandarizar nombres de columnas
                    df.columns = df.columns.str.strip().str.upper()

                    # Filtrar por PROC válido si existe esa columna
                    if "PROC" not in df.columns:
                        raise ValueError("Columna 'PROC' no encontrada en el archivo")

                    df["PROC"] = df["PROC"].astype(str).str.upper()
                    df_filtrado = df[df["PROC"].isin(proc_validos)]

                    # Extraer fecha del nombre del archivo
                    match = re.search(r'RF(\d{2})(\d{2})(\d{2})', archivo.name.upper())
                    if match:
                        dia, mes, anio = match.groups()
                        fecha = f"20{anio}-{mes}-{dia}"
                        df_filtrado["FECHA"] = pd.to_datetime(fecha)
                    else:
                        raise ValueError("Nombre de archivo no contiene una fecha válida")

                    data.append(df_filtrado)

                except Exception as e:
                    print(f"Error con {archivo.name}: {e}")

    if not data:
        raise ValueError("No se pudieron leer archivos válidos. Verificá los formatos.")

    df_final = pd.concat(data, ignore_index=True)
    return df_final
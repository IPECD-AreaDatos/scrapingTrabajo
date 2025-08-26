from etl_modular.utils.db import ConexionBaseDatos

def load_dnrpa_data(df, conexion):
    print("💾 Iniciando carga a la base de datos...")
    
    if df.empty:
        print("⚠️ DataFrame vacío, no se cargará nada.")
        return

    # Usas load_if_newer o un método similar para cargar el DF
    exito = conexion.load_if_newer(
        df,
        table_name="dnrpa", # Asumiendo que la tabla se llama 'dnrpa'
        date_column='fecha'
    )

    if exito:
        print("✅ Datos de DNRPA cargados correctamente.")
    else:
        print("⚠️ No se detectaron datos más recientes. No se cargó nada.")
    
    return exito
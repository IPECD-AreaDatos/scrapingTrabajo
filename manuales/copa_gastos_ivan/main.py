import os
import glob
import pandas as pd
from transform_data import TransformData
from connect_db import ConnectDB

def main():
    print("=== Iniciando Proceso de Carga: Copa Gastos (Manuales) ===")

    # 1. Rutas
    base_dir = os.path.dirname(os.path.abspath(__file__))
    files_dir = os.path.join(base_dir, "files")
    excel_files = glob.glob(os.path.join(files_dir, "*.xls"))

    if not excel_files:
        print(f"No se encontraron archivos .xls en {files_dir}")
        return

    print(f"Se encontraron {len(excel_files)} archivos para procesar.")

    # 2. Inicializar componentes
    transformer = TransformData()
    db = ConnectDB()

    if not db.connect():
        return

    # 3. Limpiar tabla (según pedido del usuario)
    db.clear_table("copa_gastos")

    # 4. Procesar y acumular datos
    all_data = []
    for file_path in excel_files:
        df = transformer.process_file(file_path)
        if df is not None and not df.empty:
            all_data.append(df)

    if not all_data:
        print("No se extrajeron datos de ningún archivo.")
        db.close()
        return

    # Consolidar
    final_df = pd.concat(all_data, ignore_index=True)
    
    # 5. Cargar en Base de Datos
    db.load_data(final_df, "copa_gastos")

    db.close()
    print("\n=== Proceso Finalizado Exitosamente ===")

if __name__ == "__main__":
    main()

import pandas as pd
import os

class readSheets:
    def readDataCopa(self):
        """Lee el archivo consolidado_copa.csv desde el subdirectorio file/."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "file", "consolidado_copa.csv")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

        df = pd.read_csv(file_path, sep=",", encoding="utf-8")
        print(f"Archivo leído correctamente. Filas: {len(df)}, Columnas: {len(df.columns)}")
        return df

import pandas as pd
import os

class readSheets:
    def __init__(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.file_path = os.path.join(self.base_path, "files", "planilla_de_personal_2004_feb2023-2.xlsx")

    def readData(self):
        """Lee el archivo Excel de plantilla de personal."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"No se encontró el archivo: {self.file_path}")
        
        print(f"Leyendo archivo: {self.file_path}")
        # Usamos engine='openpyxl' si es necesario, pero pandas suele detectarlo.
        df = pd.read_excel(self.file_path)
        return df

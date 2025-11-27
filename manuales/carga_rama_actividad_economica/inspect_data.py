import pandas as pd
import os
from dotenv import load_dotenv
from pymysql import connect
from sqlalchemy import create_engine
import logging

def inspect_excel_file():
    """Inspeccionar la estructura del archivo Excel"""
    print("🔍 Inspeccionando archivo Excel...")
    
    # Ruta del archivo
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_archivo = os.path.join(directorio_actual, 'files', 'Rama de Actividad Económica por municipios 2022.xlsx')
    
    # Leer el archivo Excel
    try:
        # Obtener las hojas disponibles
        xls = pd.ExcelFile(ruta_archivo)
        print(f"Hojas disponibles: {xls.sheet_names}")
        
        # Leer la primera hoja para inspeccionar
        df = pd.read_excel(ruta_archivo, sheet_name=0)
        print(f"\nDimensiones: {df.shape}")
        print(f"Columnas: {list(df.columns)}")
        
        # Mostrar las primeras filas
        print("\nPrimeras 5 filas:")
        print(df.head())
        
        # Mostrar información sobre los datos
        print("\nInformación del DataFrame:")
        print(df.info())
        
        return df
        
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

if __name__ == "__main__":
    inspect_excel_file()
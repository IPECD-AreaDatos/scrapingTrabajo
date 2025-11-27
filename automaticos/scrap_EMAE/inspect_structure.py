import pandas as pd
import os

def inspect_excel_structure():
    # Ruta del archivo
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'emae.xls')
    
    print("🔍 Inspeccionando estructura del archivo EMAE...")
    
    # Leer archivo completo sin skiprows
    df_raw = pd.read_excel(file_path, sheet_name=0)
    print(f"Dimensiones del archivo: {df_raw.shape}")
    
    # Mostrar las primeras 15 filas para entender la estructura
    print("\nPrimeras 15 filas del archivo:")
    print("=" * 80)
    
    for i in range(min(15, len(df_raw))):
        row = df_raw.iloc[i]
        print(f"Fila {i:2d}: ", end="")
        
        # Mostrar los primeros 10 valores de cada fila
        values = []
        for j in range(min(10, len(row))):
            val = row.iloc[j]
            if pd.isna(val):
                values.append("NaN")
            else:
                val_str = str(val)
                if len(val_str) > 15:
                    val_str = val_str[:12] + "..."
                values.append(val_str)
        
        print(" | ".join(values))
        
        # Analizar esta fila buscando patrones
        numeric_years = []
        month_indicators = []
        
        for val in row.values:
            if pd.notna(val):
                val_str = str(val)
                # Buscar años
                if val_str.replace('.0', '').isdigit():
                    try:
                        year_val = int(float(val_str))
                        if year_val >= 2020 and year_val <= 2030:
                            numeric_years.append(year_val)
                    except:
                        pass
                
                # Buscar meses
                month_keys = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 
                             'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
                for month in month_keys:
                    if month in val_str.lower():
                        month_indicators.append(month)
                        break
        
        if numeric_years:
            print(f"         -> AÑOS encontrados: {numeric_years}")
        if month_indicators:
            print(f"         -> MESES encontrados: {month_indicators}")
        
        print()
    
    print("\n" + "=" * 80)
    print("Análisis de columnas:")
    for i in range(min(20, df_raw.shape[1])):
        col_data = df_raw.iloc[:, i].dropna()
        if not col_data.empty:
            print(f"Columna {i}: {len(col_data)} valores no nulos")
            unique_vals = col_data.unique()[:5]  # Primeros 5 valores únicos
            print(f"  Muestras: {unique_vals}")

if __name__ == "__main__":
    inspect_excel_structure()
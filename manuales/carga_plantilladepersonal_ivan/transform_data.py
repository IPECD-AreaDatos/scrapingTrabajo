import pandas as pd

class transformData:
    def processData(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforma el DataFrame: renombra columnas por posición y limpia valores."""
        
        # 1. Mapeo por posición (0-17)
        new_columns = [
            "cod_liq", "liquidacion", "anio", "mes", "cod_jur", "jurisdiccion", 
            "cod_tipo", "tipo", "permanente_importe", "permanente_total", 
            "temporario_importe", "temporario_total", "contratados_importe", 
            "contratados_total", "otros_importe", "otros_total", "importe_gral", 
            "total_gral"
        ]
        
        # Asignamos los nuevos nombres de columnas basándonos en la posición
        # Asegurándonos de que el número de columnas coincida
        if len(df.columns) >= len(new_columns):
            df = df.iloc[:, :len(new_columns)]
            df.columns = new_columns
        else:
            print(f"Advertencia: El DataFrame tiene menos columnas ({len(df.columns)}) de las esperadas ({len(new_columns)}).")

        # 3. Convertir columnas numéricas: rellenar vacíos con 0
        int_cols = ["cod_liq", "anio", "mes", "cod_jur", "cod_tipo", "permanente_total", "temporario_total", "contratados_total", "otros_total", "total_gral"]
        float_cols = ["permanente_importe", "temporario_importe", "contratados_importe", "otros_importe", "importe_gral"]

        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)

        print(f"Transformación completada. Filas procesadas: {len(df)}")
        return df

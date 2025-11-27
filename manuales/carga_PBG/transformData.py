import pandas as pd

class transformData:
    def processData(self, df):
        # Corregir valores numéricos
        df = self.correctValor(df)
        
        # Dividir por frecuencia
        df_anual, df_trimestral = self.splitByFrequency(df)
        
        # Filtrar columnas para el DataFrame trimestral y anual
        df_trimestral = df_trimestral[['Año', 'Trimestre', 'Frecuencia', 'Variable', 'Actividad', 'Valor']]
        df_anual = df_anual[['Año', 'Frecuencia', 'Variable', 'Actividad', 'Valor']]

        return df_anual, df_trimestral

    def correctValor(self, df):
        if 'Valor' in df.columns:
            df['Valor'] = df['Valor'].round(2)
            return df
        else:
            raise KeyError("La columna 'Valor' no existe en el DataFrame.")

    def splitByFrequency(self, df):
        if 'Frecuencia' not in df.columns:
            raise KeyError("La columna 'Frecuencia' no existe en el DataFrame.")
        
        df_anual = df[df['Frecuencia'] == 'Anual'].reset_index(drop=True)
        df_trimestral = df[df['Frecuencia'] == 'Trimestral'].reset_index(drop=True)

        # Dividir columna Periodo en Año y Trimestre
        df_trimestral[['Año', 'Trimestre']] = df_trimestral['Periodo'].str.extract(r'(\d{4}) - (.*)')
        df_anual['Año'] = df_anual['Periodo'].astype(str)

        return df_anual, df_trimestral

    def calcular_variacion_anual(self, df):
        # Ordenar por año para asegurar el cálculo correcto
        df = df.sort_values(by='Año').reset_index(drop=True)

        # Calcular la variación interanual
        df['Variacion (%)'] = ((df['Valor'] / df['Valor'].shift(1)) - 1) * 100

        return df
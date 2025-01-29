class transformData:
    def correctValor(self, df):
        if 'Valor' in df.columns:
            df['Valor'] = df['Valor'].round(2)
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

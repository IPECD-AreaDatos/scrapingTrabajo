import pandas as pd

class transformData:
    def correctValor(self, df):
        df['Valor'] = round(df['Valor'], 2)
        
    def correctPeriodo(self, df):
        df_anual = df[df['Frecuencia'] == 'Anual']
        return df_anual
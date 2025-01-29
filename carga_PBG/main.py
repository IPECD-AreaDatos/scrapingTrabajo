import os
from read_csv import readSheets
from transformData import transformData

if __name__ == '__main__':
    df = readSheets().readData()
    
    transformer = transformData()
    transformer.correctValor(df)
    df_anual, df_trimestral = transformer.splitByFrequency(df)

    print("Datos Anuales con Año:")
    print(df_anual[['Año', 'Frecuencia', 'Valor']])
    print("\nDatos Trimestrales con Año y Trimestre:")
    print(df_trimestral[['Año', 'Trimestre', 'Frecuencia', 'Valor']])

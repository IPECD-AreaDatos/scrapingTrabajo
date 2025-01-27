import os
from read_csv import readSheets
from transformData import transformData

if __name__ == '__main__':
    df = readSheets().readData()
    transformData().correctValor(df)
    df_anual = transformData().correctPeriodo(df)
    print(df_anual)
    
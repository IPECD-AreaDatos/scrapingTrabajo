class Transformer:

    def __init__(self):
        
        pass

    def transformar_cantidad_vehiculos(self,df):

        df['cantidad'] = df['cantidad'].str.replace(".","")
        df['cantidad'] = df['cantidad'].astype(int)  # Convertir a tipo entero

        return df



import pandas as pd 
import pdfplumber
import re

class readDataPDF:
    def leer_datos(self, pdf_path):
        datos = []
        fila_actual = None

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text().splitlines()

                # Omitir las primeras 7 líneas de cada página
                text_sin_las_primeras_7_lineas = text[9:-1]

                for linea in text_sin_las_primeras_7_lineas:
                    # Verificar si la línea empieza con un número de 7 dígitos seguido de una letra
                    if re.match(r"12\d{5}[A-Z]", linea[:]):
                        # Si hay una fila actual, la añadimos a los datos
                        if fila_actual:
                            datos.append(fila_actual)
                        
                        # Iniciar una nueva fila
                        fila_actual = linea.split(maxsplit=14)

                        # Reemplazar valores vacíos en las columnas específicas con None
                        for i in [5, 6, 7, 8, 9, 10]:  # Índices de las columnas "Precio", "P.Norm.", "Ant Precio", "Ant P.Norm."
                            if i < len(fila_actual) and fila_actual[i] == "":
                                fila_actual[i] = None
                            
                    elif fila_actual:
                        # Concatenar la línea actual a la columna "Atributos" de la fila actual
                        fila_actual[-1] += " " + linea.strip()

        # Añadir la última fila procesada
        if fila_actual:
            datos.append(fila_actual)

        # Definir las columnas del DataFrame en el orden especificado
        columnas = ["Inf.", "For", "Pan", "Vis", "Raz For", "Raz Obs", 
                    "Precio", "P.Norm.", "Ant Precio", "Ant P.Norm.", 
                    "Var.", "T.", "TA", "Atributos"]

        # Crear el DataFrame con los datos extraídos
        df = pd.DataFrame(datos, columns=columnas)

        return df


df = readDataPDF().leer_datos('C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_canasta_basica.py\\files\\ACEITE_GIRASOL.PDF')
print(df)
print(df[["Inf.", "For", "Pan", "Vis", "Raz For", "Raz Obs","Precio", "P.Norm.", "Ant Precio", "Ant P.Norm.", "Var.", "T.", "TA", "Atributos", "otro"]])
print(df.columns)
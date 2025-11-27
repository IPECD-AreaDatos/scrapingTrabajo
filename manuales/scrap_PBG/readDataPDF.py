import pandas as pd
import pdfplumber
import re  # Módulo para expresiones regulares
from decimal import Decimal

# Diccionario para mapear los nombres de los meses a sus valores numéricos
meses = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12
}

# Preparar listas vacías para recoger los datos
data = {
    "gastos_en_personal": 0,
    "bienes_de_consumo": 0,
    "servicios_no_personales": 0,
    "bienes_de_uso": 0,
    "mes": [],
    "año": [],
    "jurisdiccion": []
}

class readDataPDF:
    def leer_datos(self, pdf_path, valor_gastos, valor_bienes, valor_servicios, valor_bien):
        line_counter = 0  # Contador para identificar la posición de la línea

        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            text = page.extract_text()
            for line in text.split("\n"):
                line_counter += 1  # Incrementa el contador para cada línea procesada
                if line_counter == 5:
                    print(line)
                    # Utiliza una expresión regular para encontrar el primer número en la línea
                    match = re.search(r'\d+', line)
                    if match:
                        jurisdiccion = int(match.group()) # Convierte el primer número encontrado a entero
                elif line.startswith("Ministerio"):
                    valor_mes = None
                    for mes, valor in meses.items():
                        if mes in line:
                            valor_mes = valor
                elif line.startswith(("100", "200", "300", "400")):
                    grupo_gasto = line.split(" ")[0]
                    valor_str = line.split()[-2]
                    # Reemplazamos adecuadamente los separadores de miles y decimales y convertimos a Decimal
                    valor = Decimal(valor_str.replace(".", "").replace(",", "."))
                    
                    if grupo_gasto == "100":
                        data['gastos_en_personal'] = valor
                    elif grupo_gasto == "200":
                        data['bienes_de_consumo'] = valor
                    elif grupo_gasto == "300":
                        data['servicios_no_personales'] = valor
                    elif grupo_gasto == "400":
                        data['bienes_de_uso'] = valor

        # Preparando datos para cargar en el DataFrame final
        carga_datos = {
            'mes': [valor_mes],
            'año': [2023],
            'jurisdiccion': [jurisdiccion],
            'gastos_en_personal': [data['gastos_en_personal']],
            'bienes_de_consumo': [data['bienes_de_consumo']],
            'servicios_no_personales': [data['servicios_no_personales']],
            'bienes_de_uso': [data['bienes_de_uso']]
        }

        # Creando el DataFrame directamente con los datos cargados
        df = pd.DataFrame(carga_datos)
        print("------------------------------------------------------------------")
        print(df)
        print("------------------------------------------------------------------")
        return df

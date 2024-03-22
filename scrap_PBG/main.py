import pdfplumber
import os 
import pandas as pd 
import re  # Módulo para expresiones regulares

# Diccionario para mapear los nombres de los meses a sus valores numéricos
meses = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
    "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
    "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12
}
# Preparar listas vacías para recoger los datos
data = {
    "Grupo Gasto": [],
    "Credito Original": [],
    "Modificaciones": [],
    "Credito Vigente": [],
    "Comprometido": [],
    "Ordenado": [],
    "Saldo": []
}

pdf_path = "C:\\Users\\Matias\\Desktop\\scrapingTrabajo\\scrap_PBG\\files\\rf667 02 01 2023.pdf"
line_counter = 0  # Contador para identificar la posición de la línea

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0]
    text = page.extract_text()
    for line in text.split("\n"):
        line_counter += 1  # Incrementa el contador para cada línea procesada
        
        # Si la línea es la quinta o comienza con "Ministerio", imprímela directamente
        if line_counter == 5:
            print(line)  # Imprime la quinta línea
            # Utiliza una expresión regular para encontrar el primer número en la línea
            match = re.search(r'\d+', line)
            if match:
                jurisdiccion = int(match.group()) # Convierte el primer número encontrado a entero
                print(jurisdiccion) 
        elif line.startswith("Ministerio"):
            # Inicializa el valor del mes como None
            valor_mes = None
            # Busca cada mes en la línea y actualiza valor_mes cuando se encuentra
            for mes, valor in meses.items():
                if mes in line:
                    valor_mes = valor
                    print(valor_mes)
   # Para otras condiciones específicas (comienza con "100", "200", "300", "400"), procesa y agrega los datos
        elif line.startswith(("100", "200", "300", "400")):
            # Dividir la línea por espacios y procesarla
            parts = line.split(" ")
            # Extraer los datos. Asumimos que la estructura es fija y conocida.
            grupo_gasto = " ".join(parts[1:-6])  # El grupo de gasto es variable en longitud
            creditos = parts[-6:]  # Los últimos 6 elementos son los números
            
            # Agregar los datos a las listas
            data["Grupo Gasto"].append(grupo_gasto)
            for key, value in zip(list(data.keys())[1:], creditos):  # Omitir "Grupo Gasto" que ya fue manejado
                # Reemplazar puntos por nada y comas por puntos para convertir a float
                clean_value = value.replace(".", "").replace(",", ".")
                data[key].append(clean_value)

# Crear el DataFrame
df_general = pd.DataFrame(data)
pd.set_option('display.float_format', '{:.2f}'.format)
df_general["Ordenado"] = df_general["Ordenado"].astype(float)
ordenado_lista = df_general["Ordenado"].tolist()
print(df_general)
print(ordenado_lista)

df = pd.DataFrame()
df['mes']= valor_mes
df['año']= 2023
df['jurisdiccion'] = jurisdiccion
df['gastos_en_personal'] = ordenado_lista[0]
df['bienes_de_consumo'] = ordenado_lista[1]
df['servicios_no_personales'] = ordenado_lista[2]
df['bienes_de_uso'] = ordenado_lista[3]
print(df)
import re
import pandas as pd

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return ''
    return (
        texto.upper()
        .replace('Á', 'A').replace('É', 'E').replace('Í', 'I')
        .replace('Ó', 'O').replace('Ú', 'U')
        .replace('.', '').replace('\n', ' ').replace('  ', ' ')
        .strip()
    )

def limpiar_nombre(nombre):
    nombre = normalizar_texto(nombre)
    nombre = re.sub(r"\d+/?", "", nombre)
    nombre = re.sub(r"[^A-ZÑ ]", "", nombre)
    nombre = nombre.replace("CDAD.", "CIUDAD")
    nombre = nombre.replace("CDAD", "CIUDAD")
    nombre = nombre.replace("AUTÓNOMA", "AUTONOMA")
    nombre = nombre.replace("AUTÓNOMA", "AUTONOMA")
    nombre = nombre.replace("CIUDAD DE BUENOS AIRES", "CIUDAD AUTONOMA DE BUENOS AIRES")
    nombre = nombre.replace("GBA", "BUENOS AIRES")
    nombre = nombre.replace(".", "")
    nombre = re.sub(r"\s+", " ", nombre)
    return nombre.strip()

def limpiar_nombree(valor):
    """Convierte a string y limpia espacios extras."""
    if pd.isna(valor):
        return None
    return str(valor).strip()

def contar_meses_validos(lista_fechas):
    """Cuenta meses hasta el primer NaN."""
    lista_cadenas = [str(el) for el in lista_fechas]
    count = 0
    for valor in lista_cadenas:
        if valor == 'nan':
            break
        count += 1
    return count
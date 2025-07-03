import re
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

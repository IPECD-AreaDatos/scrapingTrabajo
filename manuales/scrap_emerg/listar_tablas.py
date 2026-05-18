import re

ruta = '/home/usuario/Escritorio/scrapingTrabajo/manuales/scrap_emerg/emergenc_base.sql'

print("Buscando nombres de tablas reales en el SQL...")
nombres_encontrados = set()

with open(ruta, 'r', encoding='utf-8', errors='ignore') as f:
    for i, linea in enumerate(f):
        # Buscamos cualquier cosa que siga a "INSERT INTO "
        match = re.search(r"INSERT INTO\s+`?(\w+)`?", linea, re.IGNORECASE)
        if match:
            nombres_encontrados.add(match.group(1))
        if i > 50000: break # Solo miramos el principio para no tardar

print("Tablas encontradas en tu archivo:")
for tabla in nombres_encontrados:
    print(f"- {tabla}")
#Este archivo tiene el objetivo de agregar la carpeta "Credenciales_folder" al path temporalmente
#El objetivo seria utilizar este script en cada main.py para que cualquier cambio de credenciales sea mucho mas rapido.

import os
import sys

# Obtiene la ruta del directorio actual (donde se encuentra el script)
directorio_actual = os.path.dirname(os.path.realpath(__file__))

# Divide la ruta en partes
partes_ruta = directorio_actual.split(os.path.sep)

# Encuentra el índice de la carpeta "scrapingTrabajo"
indice_scraping_trabajo = partes_ruta.index("scrapingTrabajo")

# Construye la ruta hasta "scrapingTrabajo"
ruta_hasta_scraping_trabajo = os.path.join(*partes_ruta[:indice_scraping_trabajo + 1])


cadena = ruta_hasta_scraping_trabajo+"\\Credenciales_folder"
cadena = cadena.replace(":",":\\")

# Agrega la ruta hasta "scrapingTrabajo" al sys.path
sys.path.insert(0, cadena)

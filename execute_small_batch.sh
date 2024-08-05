#!/bin/bash

# Obtener el directorio del script
script_dir=$(dirname "$0") #RUTA DE DIRECTORIO
path_dir=$(realpath "$script_dir") #RUTA REAL DEL DIRECTORIO - Nos servira para volver al proyecto una vez activado el entorno


# ============= ACTIVACION DE ENTORNO ============= #

#== Retornamos a carpetas anteriores para activar el entorno

cd .. #Volvemos a carpeta SRC
cd .. #Volvemos a la carpeta que contiene a "ENV" (Entorno)

#Activamos el entorno
source env/bin/activate

#Volvemos a la carpeta del proyecto
cd $path_dir


# ============= EJECUCION DE SCRIPTS ============= #


# === CONSTRUCCION DE RUTAS

path_dnrpa="$script_dir/DNRPA/" # (DNRPA) Registro automotor
path_sipa="$script_dir/scrap_SIPA/" #(SIPA) Nivel de EMPLEO

# Crear un array con las rutas - se usara para recorrer las carpetas
paths=("$path_dnrpa" "$path_sipa")


# Recorremos el array para ejecutar cada main
for path in "${paths[@]}"; do

    echo "Procesando la ruta: $path"

    # Construir la ruta completa al script main.py
    script_path="${path}main.py"

    # Verificar si el archivo main.py existe y ejecutarlo
    if [ -f "$script_path" ]; then
        echo "Ejecutando $script_path"

        python3 "$script_path"

    else
        echo "No se encontró el archivo $script_path"
    fi
done
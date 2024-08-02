#!/bin/bash

# Obtener el directorio del script
script_dir=$(dirname "$0")

# Cambiar al directorio del script
cd "$script_dir"

# Imprimir las carpetas en el directorio actual
for dir in */ ; do
    if [ -d "$dir" ]; then
        echo "$dir"
    fi
done

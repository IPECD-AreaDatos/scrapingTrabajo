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

# Función para manejar errores
try() {
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "Error con comando: $1" >&2
    fi
    return $status
}

# === CONSTRUCCION DE RUTAS

path_ipc="$script_dir/scrap_IPC/" #(IPC) Indice de precios al Consumidor
path_sipa="$script_dir/scrap_SIPA/" #(SIPA) Nivel de EMPLEO
path_cba_cbt="$script_dir/scrap_CBT/" #(CBT y CBA) Canasta basica y total
path_supermercado="$script_dir/scrap_semaforo/" # Semaforo de Indicadores de Corientes
path_ripte="$script_dir/scrap_RIPTE/" # (RIPTE) Remuneracion Imponible Promedio
path_dnrpa="$script_dir/DNRPA/" #(DNRPA) Registro automotor
path_ipi="$script_dir/scrap_IPI/" #(IPI) Indice manufacturero NACION
path_ipicorr="$script_dir/IPICORR/" #Indice manufacturareo en corrientes


# Crear un array con las rutas - se usara para recorrer las carpetas
paths=("$path_ipc" "$path_sipa" "$path_cba_cbt" "$path_supermercado" "$path_ripte"  "$path_dnrpa" "$path_ipi" "$path_ipicorr") 


#FUNCION QUE CONTROLARA LOS ERRORES
try() {
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "Error con comando: $1" >&2
    fi
    return $status
}


# Recorremos el array para ejecutar cada main
for path in "${paths[@]}"; do

    echo "Procesando la ruta: $path"

    # Construir la ruta completa al script main.py
    script_path="${path}main.py"

    # Verificar si el archivo main.py existe y ejecutarlo
    if [ -f "$script_path" ]; then

        echo
        echo
        echo " ################################################### "

        echo "Ejecutando $script_path"
        try python3 "$script_path"
        
        echo " ################################################### "


    else
        echo "No se encontró el archivo $script_path"
    fi
done

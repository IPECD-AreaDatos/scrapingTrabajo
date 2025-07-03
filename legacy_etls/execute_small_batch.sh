#!/bin/bash
#Tener en cuenta quee este es el small batch, el mismo ejecuta scripts "cortos", de poca duracion de ejecucion.


#PASOS PARA CARGAR UN SCRIPT NUEVO A LAS AUTOMATIZACIONES
#
#1) Añadir una variable nueva en la seccion de "CONSTRUCCION DE RUTAS"
#   La variable tiene que ser representativa de la carpeta a la que hacemos referencia.
#   Ejemplo: 
#   creamos una nueva carpeta con propositos ETL, llamada nueva_carpeta    
#   haremos:
#       a) crear variable AL FINAL: path_nueva_carpeta="$script_dir/nueva_carpeta/"
#       b) añadir al final de la lista paths la nueva variable creada, el formato "$path_nueva_carpeta"

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
path_ipi="$script_dir/scrap_IPI/" #(IPI) Indice manufacturero NACION
path_ipicorr="$script_dir/IPICORR/" #Indice manufacturareo en corrientes
path_smvm= "$script_dir/scrap_SalarioMVM/" # Salario Minimo Vital y Movil
path_dnrpa="$script_dir/DNRPA/" #(DNRPA) Registro automotor
path_emae="$script_dir/scrap_EMAE/" #(EMAE) Estimador mensual de act. economica
path_eph="$script_dir/scrap_EPH/" #(EPH) Encuenta permanente de hogares
path_ieric="$script_dir/scrap_IERIC/" #(IERIC) Instituto de Estadística y Registro de la Industria de la Construcción
path_ipc_rem="$script_dir/scrap_REM/" #(REM) Expetativas de inflacion del BCRA
path_indice_salarios="$script_dir/scrap_Indice_Salarios/" #Indice de salarios
path_ipc_caba="$script_dir/scrap_IPC_CABA/" #Inflacion de CABA
path_ipc_online="$script_dir/scrap_IPC_Online/" #Inflacion pronosticada
path_ecv="$script_dir/scrap_ECV/" # (ECV) Encuentas de Calidad de Vida
path_anac="$script_dir/scrap_ANAC/" #(ANAC) Administración Nacional de Aviación Civil

# Crear un array con las rutas - se usara para recorrer las carpetas
paths=("$path_ipc" "$path_sipa" "$path_cba_cbt" "$path_supermercado" "$path_ripte" "$path_ipi" "$path_ipicorr" "$path_smvm" "$path_dnrpa" "$path_emae" "$path_eph" "$path_ieric" "$path_ipc_rem" "$path_indice_salarios" "$path_ipc_caba" "$path_ipc_online" "$path_ecv" "$path_anac")


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

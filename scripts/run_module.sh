#!/bin/bash
# Script para ejecutar un módulo ETL desde EC2
# Uso: ./run_module.sh <nombre_modulo>
# Ejemplo: ./run_module.sh ventas_combustible

set -e  # Salir si hay error

MODULE_NAME=$1

if [ -z "$MODULE_NAME" ]; then
    echo "Error: Debes proporcionar el nombre del módulo"
    echo "Uso: $0 <nombre_modulo>"
    echo "Módulos disponibles:"
    ls -d modules/*/ | sed 's/modules\///' | sed 's/\///'
    exit 1
fi

# Directorio del proyecto (ajustar según ubicación en EC2)
PROJECT_DIR="/opt/etl_scraping"
cd "$PROJECT_DIR" || exit 1

# Activar entorno virtual (ajustar ruta si es necesario)
source venv/bin/activate

# Ejecutar pipeline del módulo
python -m "modules.${MODULE_NAME}.pipeline"

# Desactivar entorno virtual
deactivate

echo "Pipeline ${MODULE_NAME} ejecutado exitosamente"






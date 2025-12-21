#!/bin/bash
# Script para ejecutar todos los módulos ETL
# Útil para ejecución manual o testing

set -e

PROJECT_DIR="/opt/etl_scraping"
cd "$PROJECT_DIR" || exit 1

source venv/bin/activate

# Obtener lista de módulos
MODULES=$(ls -d modules/*/ | sed 's/modules\///' | sed 's/\///')

echo "=== Ejecutando todos los módulos ETL ==="
echo "Módulos encontrados: $MODULES"
echo ""

for module in $MODULES; do
    echo "----------------------------------------"
    echo "Ejecutando módulo: $module"
    echo "----------------------------------------"
    
    if python -m "modules.${module}.pipeline"; then
        echo "✓ Módulo $module completado exitosamente"
    else
        echo "✗ Error en módulo $module"
        # Continuar con el siguiente módulo
    fi
    
    echo ""
done

deactivate

echo "=== Ejecución de todos los módulos completada ==="






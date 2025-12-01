#!/bin/bash
# Script para listar scrapers manuales disponibles
# Uso: ./scripts/execute_manuales.sh [nombre_scraper]

set -e

# Obtener el directorio del script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Cambiar al directorio del proyecto
cd "$PROJECT_DIR"

# Activar entorno virtual
if [ -d "env_scrapping" ]; then
    source env_scrapping/bin/activate
elif [ -d "env" ]; then
    source env/bin/activate
else
    echo "⚠️ No se encontró entorno virtual. Continuando sin activar..."
fi

# Lista de scrapers manuales disponibles
MANUALES=(
    "manuales/scrap_ECV/main.py"
    "manuales/scrap_Censos/main.py"
    "manuales/scrap_Censo_IPECD/main.py"
    "manuales/scrap_Censo_IPECD_Jose/main.py"
    "manuales/scrap_Censo_Municipio/main.py"
    "manuales/scrap_nacion_nea_Jose/main.py"
    "manuales/carga_PBG/main.py"
    "manuales/carga_diccionario_clae/main.py"
    "manuales/carga_rama_actividad_economica/main.py"
    "manuales/script_PuestosCadaMilHabitantes/main.py"
    "manuales/scrap_EPH/main.py"
    "manuales/scrap_IPC_tabla/main.py"
    "manuales/scrap_PBG/main.py"
    "manuales/scrap_ReconocimientoMedicos/main.py"
    "manuales/scrap_SISPER/main.py"
)

# Si se pasa un argumento, ejecutar ese scraper específico
if [ $# -gt 0 ]; then
    SCRAPER_BUSCADO="$1"
    ENCONTRADO=false
    
    for scraper in "${MANUALES[@]}"; do
        if [[ "$scraper" == *"$SCRAPER_BUSCADO"* ]]; then
            if [ -f "$scraper" ]; then
                echo "🚀 Ejecutando: $scraper"
                echo "=========================================="
                python3 "$scraper"
                ENCONTRADO=true
                break
            fi
        fi
    done
    
    if [ "$ENCONTRADO" = false ]; then
        echo "❌ No se encontró el scraper: $SCRAPER_BUSCADO"
        echo ""
        echo "Scrapers disponibles:"
        for scraper in "${MANUALES[@]}"; do
            echo "  - $(basename $(dirname $scraper))"
        done
        exit 1
    fi
else
    # Mostrar menú de scrapers disponibles
    echo "📋 SCRAPERS MANUALES DISPONIBLES"
    echo "=========================================="
    echo ""
    echo "Ejecuta un scraper específico con:"
    echo "  ./scripts/execute_manuales.sh [nombre]"
    echo ""
    echo "Scrapers disponibles:"
    echo ""
    
    for i in "${!MANUALES[@]}"; do
        scraper="${MANUALES[$i]}"
        nombre=$(basename $(dirname $scraper))
        if [ -f "$scraper" ]; then
            echo "  $((i+1)). $nombre ✅"
        else
            echo "  $((i+1)). $nombre ⚠️ (no encontrado)"
        fi
    done
    
    echo ""
    echo "Ejemplo:"
    echo "  ./scripts/execute_manuales.sh scrap_ECV"
    echo "  ./scripts/execute_manuales.sh carga_PBG"
fi









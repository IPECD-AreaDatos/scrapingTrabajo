#!/bin/bash
# Script para configurar cron jobs en EC2
# Ejecutar como: sudo ./setup_cron.sh

PROJECT_DIR="/opt/etl_scraping"
CRON_LOG_DIR="${PROJECT_DIR}/logs/cron"

# Crear directorio de logs de cron
mkdir -p "$CRON_LOG_DIR"

# Archivo temporal para cron
CRON_TEMP=$(mktemp)

# Obtener crontab actual
crontab -l > "$CRON_TEMP" 2>/dev/null || true

# Agregar comentario
echo "" >> "$CRON_TEMP"
echo "# ETL Pipelines - Actualizado $(date)" >> "$CRON_TEMP"
echo "" >> "$CRON_TEMP"

# Ejemplo: Ejecutar ventas_combustible todos los días a las 2 AM
echo "0 2 * * * cd ${PROJECT_DIR} && ${PROJECT_DIR}/venv/bin/python -m modules.ventas_combustible.pipeline >> ${CRON_LOG_DIR}/ventas_combustible.log 2>&1" >> "$CRON_TEMP"

# Ejemplo: Ejecutar todos los módulos los domingos a las 3 AM
# echo "0 3 * * 0 cd ${PROJECT_DIR} && ${PROJECT_DIR}/scripts/run_all_modules.sh >> ${CRON_LOG_DIR}/all_modules.log 2>&1" >> "$CRON_TEMP"

# Instalar crontab
crontab "$CRON_TEMP"
rm "$CRON_TEMP"

echo "Cron jobs configurados exitosamente"
echo "Ver crontab con: crontab -l"
echo "Logs en: ${CRON_LOG_DIR}"




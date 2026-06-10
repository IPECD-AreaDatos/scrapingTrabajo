#!/bin/bash
# =============================================================================
# backup_salud.sh
# Backup diario de tablas críticas - Base de datos: salud
# Tablas: seguimientos, logs_actividad_dashboard, contactos_audit
#
# Ejecución: cron todos los días a las 23:00
# Retención local: 14 días
# Retención Google Drive: anual (vía rclone)
# =============================================================================

# --- CONFIGURACIÓN -----------------------------------------------------------
DB_HOST="149.50.145.182"
DB_PORT="5432"
DB_USER="IPECD_Manuela"
DB_NAME="salud"

TABLAS=("seguimientos" "logs_actividad_dashboard" "contactos_audit")

LOCAL_BACKUP_DIR="/var/backups/salud_embarazo"
RETENTION_DIAS=14

# Nombre del remote configurado en rclone (ver instrucciones abajo)
RCLONE_REMOTE="gdrive_salud"
RCLONE_CARPETA="backups_salud_embarazo"

LOG_FILE="/var/log/backup_salud.log"

# --- CONTRASEÑA (via .pgpass, NO se pone acá - ver instrucciones) ------------
# El archivo ~/.pgpass debe existir con este contenido:
# 149.50.145.182:5432:salud:IPECD_Manuela:IPECDatos.2026
# Permisos: chmod 600 ~/.pgpass

# --- INICIO ------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FECHA_LEGIBLE=$(date "+%d/%m/%Y %H:%M:%S")
BACKUP_FILE="${LOCAL_BACKUP_DIR}/salud_tablas_${TIMESTAMP}.dump"

log() {
    echo "[${FECHA_LEGIBLE}] $1" | tee -a "$LOG_FILE"
}

log "========================================================"
log "Iniciando backup de base de datos: ${DB_NAME}"
log "Tablas: ${TABLAS[*]}"
log "========================================================"

# Crear directorio si no existe
mkdir -p "$LOCAL_BACKUP_DIR"

# Construir los flags -t para cada tabla
T_FLAGS=""
for tabla in "${TABLAS[@]}"; do
    T_FLAGS="$T_FLAGS -t $tabla"
done

# --- EJECUTAR pg_dump --------------------------------------------------------
log "Ejecutando pg_dump..."

PGPASSFILE=~/.pgpass pg_dump \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -F c \
    -Z 9 \
    $T_FLAGS \
    -f "$BACKUP_FILE" \
    "$DB_NAME"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    TAMANIO=$(du -sh "$BACKUP_FILE" | cut -f1)
    log "✓ Backup local exitoso: ${BACKUP_FILE} (${TAMANIO})"
else
    log "✗ ERROR: pg_dump falló con código ${EXIT_CODE}"
    log "Revisar conexión y credenciales. Abortando."
    exit 1
fi

# --- SUBIR A GOOGLE DRIVE con rclone -----------------------------------------
log "Subiendo a Google Drive (${RCLONE_REMOTE}:${RCLONE_CARPETA})..."

rclone copy "$BACKUP_FILE" "${RCLONE_REMOTE}:${RCLONE_CARPETA}/" \
    --log-level INFO \
    --log-file "$LOG_FILE"

if [ $? -eq 0 ]; then
    log "✓ Subida a Google Drive exitosa"
else
    log "⚠ ADVERTENCIA: Falló la subida a Google Drive. El backup local sí existe."
    # No abortamos porque el backup local está OK
fi

# --- LIMPIEZA LOCAL (mantener solo 14 días) -----------------------------------
log "Limpiando backups locales de más de ${RETENTION_DIAS} días..."
ELIMINADOS=$(find "$LOCAL_BACKUP_DIR" -name "salud_tablas_*.dump" -mtime +${RETENTION_DIAS} -print)

if [ -n "$ELIMINADOS" ]; then
    find "$LOCAL_BACKUP_DIR" -name "salud_tablas_*.dump" -mtime +${RETENTION_DIAS} -delete
    log "Archivos eliminados:"
    echo "$ELIMINADOS" | while read -r f; do log "  - $f"; done
else
    log "No hay backups viejos que eliminar"
fi

# --- RESUMEN LOCAL -----------------------------------------------------------
log "Backups locales actuales:"
ls -lh "${LOCAL_BACKUP_DIR}"/salud_tablas_*.dump 2>/dev/null | \
    awk '{print "  " $NF " (" $5 ")"}' | tee -a "$LOG_FILE"

log "========================================================"
log "Backup finalizado correctamente"
log "========================================================"

exit 0

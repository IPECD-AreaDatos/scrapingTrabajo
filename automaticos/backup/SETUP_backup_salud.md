# INSTRUCCIONES DE SETUP — backup_salud.sh
# Para: Mati (administrador del servidor)
# Base: salud | Servidor: 149.50.145.182

## PASO 1 — Configurar contraseña de Postgres (sin que el cron la pida)

Crear el archivo .pgpass en el home del usuario que correrá el cron:

    echo "149.50.145.182:5432:salud:IPECD_Manuela:IPECDatos.2026" >> ~/.pgpass
    chmod 600 ~/.pgpass

Probar que funciona sin password:

    psql -h 149.50.145.182 -p 5432 -U IPECD_Manuela -d salud -c "\dt"


## PASO 2 — Copiar el script al servidor

    scp backup_salud.sh usuario@149.50.145.182:/opt/scripts/backup_salud.sh
    chmod +x /opt/scripts/backup_salud.sh

Crear el directorio de backups y el de logs:

    mkdir -p /var/backups/salud_embarazo
    touch /var/log/backup_salud.log


## PASO 3 — Instalar rclone y conectarlo a Google Drive

    curl https://rclone.org/install.sh | sudo bash

Configurar el remote (esto abre un wizard interactivo, necesita navegador la primera vez):

    rclone config
    # → New remote → nombre: gdrive_salud → tipo: drive → seguir el wizard OAuth

Probar la conexión:

    rclone ls gdrive_salud:


## PASO 4 — Activar en cron (todos los días a las 23:00)

    crontab -e

Agregar esta línea:

    0 23 * * * /opt/scripts/backup_salud.sh >> /var/log/backup_salud.log 2>&1


## PASO 5 — Si usás Airflow en vez de cron

En Airflow, crear un DAG con BashOperator que ejecute:

    /opt/scripts/backup_salud.sh

Schedule: "0 23 * * *"  (mismo horario)


## PASO 6 — Probar manualmente antes de dejar automático

    /opt/scripts/backup_salud.sh

Verificar que:
  ✓ Aparece el .dump en /var/backups/salud_embarazo/
  ✓ Aparece el archivo en Google Drive (carpeta backups_salud_embarazo)
  ✓ El log en /var/log/backup_salud.log muestra éxito


## RESTAURAR una tabla desde el backup (cuando se necesite)

    pg_restore \
        -h 149.50.145.182 -p 5432 \
        -U IPECD_Manuela \
        -d salud \
        -t seguimientos \          # <-- tabla que querés restaurar
        --data-only \              # solo datos, no estructura
        salud_tablas_20260610_230001.dump


## ESTRUCTURA DE ARCHIVOS EN EL SERVIDOR

    /opt/scripts/
        backup_salud.sh            ← el script

    /var/backups/salud_embarazo/
        salud_tablas_20260610_230001.dump
        salud_tablas_20260611_230001.dump
        ...  (se mantienen 14 días locales)

    /var/log/
        backup_salud.log           ← log acumulativo de todas las ejecuciones

    Google Drive (gdrive_salud):
        backups_salud_embarazo/
            salud_tablas_YYYYMMDD_HHMMSS.dump
            ...  (retención anual, no se borran automáticamente)

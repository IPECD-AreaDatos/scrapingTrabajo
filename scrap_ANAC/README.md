# ANAC - Scraper de Estadísticas Aeroportuarias

Este script automatiza la descarga y procesamiento de estadísticas de movimientos aeroportuarios de la Administración Nacional de Aviación Civil (ANAC) de Argentina.

## Funcionalidades

- **Descarga automática** de archivos comprimidos desde el portal de ANAC
- **Extracción inteligente** del archivo Excel más reciente (2023-2025)
- **Procesamiento de datos** específico de la "TABLA 11"
- **Carga automática** en base de datos MySQL
- **Integración** con Google Sheets para reporting

## Requisitos

### Dependencias Python
```bash
pip install requests urllib3 zipfile pandas pymysql sqlalchemy google-api-python-client python-dotenv
```

### Variables de entorno
Crear archivo `.env` con:
```
HOST_DBB=tu_host_mysql
USER_DBB=tu_usuario_mysql  
PASSWORD_DBB=tu_contraseña_mysql
NAME_DBB_DATALAKE_ECONOMICO=nombre_base_datos
GOOGLE_SHEETS_API_KEY={"type": "service_account", ...}
```

## Uso

```bash
python main.py
```

## Estructura del proyecto

- `main.py` - Script principal
- `home_page.py` - Descarga y extracción de archivos
- `anac_armadoDF.py` - Procesamiento de datos Excel  
- `loadDatabase.py` - Conexión y operaciones MySQL
- `save_data_sheet.py` - Integración Google Sheets
- `files/` - Directorio para archivos descargados

## Flujo del proceso

1. Descarga archivo ZIP desde ANAC
2. Extrae archivo Excel más reciente
3. Busca y procesa "TABLA 11"
4. Aplica correcciones específicas a los datos
5. Carga datos en MySQL
6. Actualiza Google Sheets con resultados

## Datos procesados

El script procesa estadísticas de:
- Movimientos aeroportuarios por provincia
- Datos de pasajeros 
- Series históricas 2023-2025 (datos más recientes)

---
*Desarrollado para IPECD - Área de Datos*
# ANAC - Scraper de Estadísticas Aeroportuarias

Scraper ETL automatizado para descargar y procesar estadísticas de movimientos aeroportuarios de la Administración Nacional de Aviación Civil (ANAC) de Argentina.

## 📋 Arquitectura ETL

El proyecto sigue una arquitectura ETL (Extract, Transform, Load) con separación clara de responsabilidades:

```
scrap_ANAC/
├── main.py              # Orquestador principal del pipeline ETL
├── README.md            # Este archivo
├── README_SELENIUM.md   # Guía de configuración Selenium (opcional)
├── etl/                 # Módulos ETL (paquete Python)
│   ├── __init__.py      # Inicialización del paquete
│   ├── extract.py       # EXTRACT: Descarga y extracción de archivos
│   ├── transform.py     # TRANSFORM: Procesamiento y limpieza de datos
│   └── load.py          # LOAD: Carga a MySQL y actualización de Google Sheets
└── files/               # Archivos descargados (temporales)
```

## 🔄 Flujo del Proceso

```
1. EXTRACT (extract.py)
   ├── Descarga archivo ZIP desde portal ANAC
   ├── Extrae archivo Excel (series-historicas-2023-2025.xlsx)
   └── Retorna ruta del archivo Excel

2. TRANSFORM (transform.py)
   ├── Lee archivo Excel
   ├── Busca y procesa "TABLA 11"
   ├── Extrae fechas reales del Excel (2023-2025)
   ├── Aplica correcciones específicas a datos
   └── Retorna DataFrame limpio y validado

3. LOAD (load.py)
   ├── Verifica si hay datos nuevos (consulta BD)
   ├── Actualiza solo datos desde 2023 (mantiene históricos)
   ├── Carga a MySQL (upsert incremental)
   └── Actualiza Google Sheets con último valor
```

## 🚀 Uso

### Ejecución básica:
```bash
python main.py
```

### Ejecución con entorno virtual:
```bash
# Activar entorno virtual
source env_scrapping/bin/activate  # Linux/Mac
# o
env_scrapping\Scripts\Activate.ps1  # Windows

# Ejecutar
python main.py
```

## 📦 Requisitos

### Dependencias Python
Ver `requirements.txt` en la raíz del proyecto. Principales:
- `requests` - Descarga de archivos
- `pandas` - Procesamiento de datos
- `openpyxl` - Lectura de Excel
- `pymysql` - Conexión MySQL
- `sqlalchemy` - ORM para MySQL
- `google-api-python-client` - Google Sheets API
- `python-dotenv` - Variables de entorno

### Variables de entorno (.env)
```env
HOST_DBB=tu_host_mysql
USER_DBB=tu_usuario_mysql
PASSWORD_DBB=tu_contraseña_mysql
NAME_DBB_DATALAKE_ECONOMICO=nombre_base_datos
GOOGLE_SHEETS_API_KEY={"type": "service_account", ...}
```

## 📊 Datos Procesados

- **Período**: 2023-01-01 hasta 2025-08-01 (33 meses)
- **Fuente**: TABLA 11 del Excel de ANAC
- **Aeropuertos**: 57 aeropuertos argentinos
- **Métricas**: Pasajeros totales por aeropuerto

### Correcciones aplicadas:
- Valores específicos corregidos en columna "corrientes"
- Validación de tipos de datos
- Limpieza de valores nulos

## 🔧 Características Técnicas

### Extract
- Descarga automática con `requests`
- Soporte opcional para Selenium (headless para Linux)
- Verificación de espacio en disco
- Cache de archivos (1 hora)
- Limpieza automática de temporales

### Transform
- Extracción inteligente de fechas desde Excel
- Detección automática de filas con datos
- Correcciones específicas de datos
- Validación de estructura

### Load
- **Actualización incremental**: Solo actualiza datos desde 2023, mantiene históricos
- Verificación de datos nuevos antes de cargar
- Upsert a MySQL (evita duplicados)
- Integración con Google Sheets
- Manejo robusto de errores

## 📝 Logs

Los logs se guardan en `../../logs/anac_scraper.log` con rotación automática:
- Máximo 5MB por archivo
- Mantiene 2 archivos de respaldo
- Formato: `YYYY-MM-DD HH:MM:SS - LEVEL - MESSAGE`

## 🐧 Compatibilidad Linux

El scraper está optimizado para ejecutarse en servidores Linux (EC2):
- Usa `requests` por defecto (no requiere navegador)
- Soporte opcional para Selenium headless
- Manejo de rutas multiplataforma
- Logging con rotación de archivos

Ver `README_SELENIUM.md` para configuración avanzada de Selenium.

## ⚠️ Notas Importantes

1. **Datos históricos**: El Excel descargado solo contiene datos desde 2023. Los datos históricos anteriores se mantienen en la BD.

2. **Actualización incremental**: El sistema solo actualiza datos desde 2023 en adelante, preservando datos históricos anteriores.

3. **Verificación de datos nuevos**: El proceso verifica si hay datos nuevos antes de cargar, evitando actualizaciones innecesarias.

4. **Google Sheets**: Solo actualiza la celda del último período, no reemplaza columnas completas.

## 🔍 Troubleshooting

### Error: "No hay datos nuevos"
- El Excel descargado no tiene fechas más recientes que la BD
- Verificar última fecha en BD vs Excel

### Error: "No se encontró TABLA 11"
- El formato del Excel puede haber cambiado
- Verificar estructura del archivo descargado

### Error de conexión a BD
- Verificar variables de entorno
- Verificar conectividad de red
- Verificar credenciales MySQL

---
*Desarrollado para IPECD - Área de Datos*

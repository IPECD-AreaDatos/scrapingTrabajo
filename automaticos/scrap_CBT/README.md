# Proyecto ETL - CBT/CBA (Canasta Básica Total y Alimentaria)

Sistema ETL para extraer, transformar y cargar datos de Canasta Básica Total (CBT) y Canasta Básica Alimentaria (CBA) desde INDEC.

## 📁 Estructura del Proyecto

```
scrap_CBT/
├── main.py                          # Orquestador principal del flujo ETL
├── extract/                         # Capa de extracción
│   ├── __init__.py
│   ├── extractor_cbt.py            # Descarga CBT.xls desde INDEC
│   └── extractor_pobreza.py        # Descarga Pobreza.xls desde INDEC
├── transform/                       # Capa de transformación
│   ├── __init__.py
│   └── transformer_cbt_cba.py      # Procesa y transforma datos
├── load/                           # Capa de carga
│   ├── __init__.py
│   ├── database_loader.py          # Carga datos a base de datos
│   └── email_sender.py             # Envía correos de notificación
├── validate/                       # Validación de datos
│   ├── __init__.py
│   └── data_validator.py           # Valida calidad de datos
├── correcciones/                   # Correcciones manuales
│   └── README.md                   # Instrucciones
└── files/                          # Archivos y datos
    └── data/                       # Datos descargados (XLS, CSV)
```

## 🚀 Uso

### Ejecución Completa

```bash
python main.py
```

Esto ejecuta el flujo ETL completo:
1. **Extract**: Descarga archivos desde INDEC
2. **Transform**: Procesa y transforma datos
3. **Validate**: Valida calidad de datos
4. **Load**: Carga a base de datos y envía correos

### Variables de Entorno

Crear un archivo `.env` con:

```env
HOST_DBB=tu_host
USER_DBB=tu_usuario
PASSWORD_DBB=tu_contraseña
NAME_DBB_DATALAKE_SOCIO=nombre_bd_datalake
NAME_DBB_DWH_SOCIO=nombre_bd_dwh
```

## 📊 Flujo de Datos

```
INDEC (Web) 
    ↓
[EXTRACT] → files/data/CBT.xls, Pobreza.xls
    ↓
[TRANSFORM] → DataFrame consolidado
    ↓
[VALIDATE] → Verificación de calidad
    ↓
[LOAD] → Base de Datos + Correos + API
```

## 🔍 Validaciones

El sistema valida automáticamente:
- ✓ Tipos de datos correctos
- ✓ Valores en rangos válidos
- ✓ Coherencia temporal
- ✓ Ausencia de duplicados
- ✓ Tendencias razonables

## 📝 Correcciones Manuales

Para realizar correcciones manuales:

1. Crear script en `correcciones/correccion_YYYY_MM_DD.py`
2. Documentar el problema y solución
3. Ejecutar manualmente cuando sea necesario

## 🛠️ Requisitos

- Python 3.8+
- Selenium
- Pandas
- PyMySQL
- python-dotenv
- ChromeDriver

## 📧 Contacto

Instituto Provincial de Estadística y Ciencia de Datos de Corrientes

# Proyecto ETL - CBT/CBA (Canasta Básica Total y Alimentaria)

Sistema ETL automatizado para extraer, transformar y cargar datos de Canasta Básica Total (CBT) y Canasta Básica Alimentaria (CBA) desde INDEC (GBA) e integrarlos con los datos del NEA desde una planilla de Google Sheets interna.

---

## 📁 Estructura del Proyecto

```text
scrap_CBT/
├── main.py                          # Orquestador principal del flujo ETL
├── etl/                             # Paquete ETL integrado
│   ├── __init__.py                  # Inicialización y exportación de clases
│   ├── extract.py                   # Extracción: Descarga CBT.xls de INDEC y parsee la fecha de publicación
│   ├── transform.py                 # Transformación: Procesa CBT.xls y lee Google Sheets para NEA
│   ├── load.py                      # Carga: Carga datos a base de datos (con control de checksum histórico)
│   └── validate.py                  # Validación: Valida consistencia y calidad de datos (minúsculas)
├── files/                          # Archivos locales de trabajo
│   └── data/                       # Almacena CBT.xls temporalmente
└── README.md                        # Documentación actualizadas

```

---

## 📊 Flujo de Datos Actualizado

1. **EXTRACT:**
   * El driver de Selenium accede a la web de INDEC y localiza el enlace de descarga del XLS mensual.
   * Lee el texto del enlace (`textContent`) para deducir la **última fecha oficial de publicación** (ej. *Abril de 2016 a abril de 2026* -> `2026-04-01`).
   * Descarga el archivo `CBT.xls` de forma local.
2. **TRANSFORM:**
   * Extrae los datos individuales y de hogares de GBA desde `CBT.xls`.
   * Se conecta de forma segura a la planilla Google Sheet `CBA y CBT mensual` usando la cuenta de servicio autorizada.
   * Limpia el formato de moneda y parsea las fechas de Google Sheets (soporta abreviaturas de 3 y 4 letras como `sept-24`).
   * Filtra los datos de Google Sheets hasta la fecha oficial de INDEC (evitando cargar borradores).
   * Une ambas fuentes por la columna `fecha` para asegurar una alineación perfecta de la serie.
3. **VALIDATE:**
   * Realiza validaciones automáticas de calidad: coherencia temporal, tipos numéricos, no duplicación, y coherencia CBA < CBT.
4. **LOAD:**
   * Conecta a la base de datos MySQL (`datalake_sociodemografico`) o PostgreSQL (`datalake_economico`) según la variable `DB_VERSION`.
   * **Control inteligente de cambios:** Compara la última fecha de registro y la **suma total histórica de los importes del NEA** (`SUM`). Si hay registros nuevos o se corrigió algún dato del pasado, la tabla se actualiza por completo de forma automática.

---

## 🚀 Uso e Instalación

### Requisitos previos
El script requiere Python 3.8+ y las siguientes librerías instaladas:
```bash
pip install pandas numpy sqlalchemy pymysql psycopg2 cryptography selenium requests google-auth google-api-python-client python-dotenv
```
*Es necesario tener instalado Google Chrome y su respectivo ChromeDriver compatible.*

### Archivo `.env` (Configuración de entorno)
En el directorio raíz del proyecto debe existir un archivo `.env` configurado con las bases de datos correspondientes y la credencial de cuenta de servicio de Google:

```env
# Configuración Base 1 (MySQL)
HOST_DBB1=tu_host
USER_DBB1=tu_usuario
PASSWORD_DBB1=tu_contraseña
PORT_DBB1=3306
NAME_DBB_DATALAKE_SOCIO=datalake_sociodemografico

# Configuración Base 2 (PostgreSQL)
HOST_DBB2=tu_host
USER_DBB2=tu_usuario
PASSWORD_DBB2=tu_contraseña
PORT_DBB2=5432
NAME_DBB_DATALAKE_ECONOMICO=datalake_economico

# Selección de Base de Datos (1 o 2)
DB_VERSION=1

# Clave de API / Cuenta de servicio de Google Sheets (en una línea)
GOOGLE_SHEETS_API_KEY={"type": "service_account", "project_id": "ipicorr", ...}
```

### Ejecutar ETL
```bash
python main.py
```
*Para cambiar de base de datos de destino, modifica el valor de `DB_VERSION` en el `.env` o en la variable de entorno del sistema.*

---

## ✉️ Contacto
**Instituto Provincial de Estadística y Ciencia de Datos de Corrientes (IPECD)**
*Departamento de Datos*

# scrapingTrabajo

Pipeline de extracción de datos económicos del IPECD.

## 🛒 Pipeline Canasta Básica

### Configuración

1. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar variables de entorno**:
   ```bash
   cp .env.example .env
   # Editar .env con tus credenciales
   ```

3. **Verificar configuración**:
   ```bash
   python verificar_setup.py
   ```

### Uso

**Ejecutar pipeline completo**:
```bash
python test_canasta_basica.py
```

**Usar desde código**:
```python
from canasta_basica.run import run_canasta_basica
run_canasta_basica()
```

### Estructura

- `canasta_basica/run.py`: Orquestador principal
- `canasta_basica/carrefour_extractor.py`: Extractor de Carrefour
- `canasta_basica/load.py`: Cargador a base de datos
- `canasta_basica/utils_db.py`: Utilidades de BD
- `canasta_basica/utils_sheets.py`: Utilidades de Google Sheets

### Características

✅ Manejo inteligente de sesiones con cookies
✅ Logging comprehensivo
✅ Manejo robusto de errores
✅ Configuración via variables de entorno
✅ Procesamiento batch de múltiples productos
✅ Guardado automático en CSV y BD

### Variables de Entorno

**Requeridas**:
- `HOST_DBB`: Host de la base de datos
- `USER_DBB`: Usuario de BD
- `PASSWORD_DBB`: Contraseña de BD
- `NAME_DBB_DATALAKE_ECONOMICO`: Nombre de la BD

**Opcionales**:
- `CARREFOUR_EMAIL`: Email para login
- `CARREFOUR_PASSWORD`: Contraseña para login
- `SHEETS_RANGE`: Rango de Google Sheets (default: 'Hoja 1!A2:K35')
- `GOOGLE_SHEETS_CREDENTIALS_FILE`: Archivo de credenciales JSON

# Estructura del Proyecto ETL - Documentación

## 📁 Estructura de Directorios

```
scrapingTrabajo/
├── config/                 # Configuración centralizada
│   ├── __init__.py
│   └── settings.py         # Settings singleton con variables de entorno
│
├── core/                   # Clases base y componentes fundamentales
│   ├── __init__.py
│   ├── base_extractor.py   # Clase base abstracta para extractores
│   ├── base_transformer.py # Clase base abstracta para transformers
│   ├── base_loader.py      # Clase base abstracta para loaders
│   └── pipeline_runner.py  # Orquestador principal de pipelines
│
├── modules/                # Módulos ETL individuales
│   └── ventas_combustible/ # Ejemplo de módulo migrado
│       ├── __init__.py
│       ├── extract.py      # Extractor específico del módulo
│       ├── transform.py    # Transformer específico del módulo
│       ├── load.py         # Loader específico del módulo
│       ├── pipeline.py     # Pipeline principal del módulo
│       └── files/          # Archivos temporales del módulo
│
├── utils/                  # Utilidades centralizadas
│   ├── __init__.py
│   ├── logger.py           # Sistema de logging con rotación
│   ├── db.py               # Utilidades de base de datos
│   ├── dates.py            # Utilidades de fechas
│   ├── mail.py             # Utilidades de correo
│   └── helpers.py          # Funciones helper generales
│
├── scripts/                # Scripts bash para ejecución en EC2
│   ├── run_module.sh       # Ejecutar un módulo específico
│   ├── run_all_modules.sh  # Ejecutar todos los módulos
│   └── setup_cron.sh       # Configurar cron jobs
│
├── web/                    # API FastAPI (futuro panel web)
│   ├── __init__.py
│   ├── main.py             # Aplicación FastAPI principal
│   └── requirements.txt    # Dependencias de FastAPI
│
├── logs/                   # Logs centralizados
│   └── execution_history.json  # Historial de ejecuciones
│
├── output/                 # Archivos de salida
│
└── automaticos/            # Módulos antiguos (a migrar)
    └── scrap_*/            # Módulos legacy
```

## 🏗️ Arquitectura

### Principios de Diseño

1. **Separación de Responsabilidades**: Cada componente tiene una responsabilidad única
2. **Herencia de Clases Base**: Todos los módulos heredan de clases base en `core/`
3. **Configuración Centralizada**: Variables de entorno y settings en `config/`
4. **Logging Centralizado**: Sistema de logging con rotación automática
5. **Registro de Ejecuciones**: Historial JSON de todas las ejecuciones

### Flujo ETL

```
PipelineRunner
    ↓
[EXTRACT] BaseExtractor → MóduloExtractor
    ↓
[TRANSFORM] BaseTransformer → MóduloTransformer
    ↓
[LOAD] BaseLoader → MóduloLoader
    ↓
Registro de Ejecución → execution_history.json
```

## 📝 Crear un Nuevo Módulo ETL

### Paso 1: Crear estructura de carpetas

```bash
mkdir -p modules/mi_nuevo_modulo/files
```

### Paso 2: Crear extract.py

```python
from core.base_extractor import BaseExtractor
from typing import Any

class MiNuevoModuloExtractor(BaseExtractor):
    def __init__(self):
        super().__init__("mi_nuevo_modulo")
    
    def extract(self, **kwargs) -> Any:
        # Tu lógica de extracción aquí
        return {"data": "datos extraídos"}
```

### Paso 3: Crear transform.py

```python
from core.base_transformer import BaseTransformer
import pandas as pd
from typing import Any

class MiNuevoModuloTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("mi_nuevo_modulo")
    
    def transform(self, data: Any, **kwargs) -> pd.DataFrame:
        # Tu lógica de transformación aquí
        return pd.DataFrame(data)
```

### Paso 4: Crear load.py

```python
from core.base_loader import BaseLoader
import pandas as pd
from typing import Dict, Optional

class MiNuevoModuloLoader(BaseLoader):
    def __init__(self, db_config: Optional[Dict[str, str]] = None):
        super().__init__("mi_nuevo_modulo", db_config)
    
    def load(self, data: pd.DataFrame, **kwargs) -> bool:
        # Tu lógica de carga aquí
        return True
```

### Paso 5: Crear pipeline.py

```python
from core.pipeline_runner import PipelineRunner
from modules.mi_nuevo_modulo.extract import MiNuevoModuloExtractor
from modules.mi_nuevo_modulo.transform import MiNuevoModuloTransformer
from modules.mi_nuevo_modulo.load import MiNuevoModuloLoader
from utils.logger import setup_logger

def main():
    logger = setup_logger('mi_nuevo_modulo')
    runner = PipelineRunner('mi_nuevo_modulo')
    
    extractor = MiNuevoModuloExtractor()
    transformer = MiNuevoModuloTransformer()
    loader = MiNuevoModuloLoader()
    
    runner.set_components(extractor, transformer, loader)
    result = runner.run()
    
    return result

if __name__ == '__main__':
    main()
```

## 🚀 Ejecución

### Ejecución Manual (Desarrollo)

```bash
# Activar entorno virtual
source venv/bin/activate

# Ejecutar un módulo
python -m modules.ventas_combustible.pipeline
```

### Ejecución en EC2 (Producción)

```bash
# Ejecutar un módulo específico
./scripts/run_module.sh ventas_combustible

# Ejecutar todos los módulos
./scripts/run_all_modules.sh

# Configurar cron jobs
sudo ./scripts/setup_cron.sh
```

### API Web (FastAPI)

```bash
# Instalar dependencias
pip install -r web/requirements.txt

# Ejecutar servidor
cd web
python main.py

# O con uvicorn directamente
uvicorn web.main:app --host 0.0.0.0 --port 8000
```

**Endpoints disponibles:**
- `GET /` - Información de la API
- `GET /api/modules` - Listar módulos disponibles
- `GET /api/modules/{module_name}/status` - Estado de última ejecución
- `POST /api/modules/{module_name}/execute` - Ejecutar un módulo
- `GET /api/modules/{module_name}/logs` - Ver logs de un módulo
- `GET /api/health` - Health check

## 📊 Registro de Ejecuciones

Todas las ejecuciones se registran en `logs/execution_history.json`:

```json
{
  "ventas_combustible": {
    "last_execution": "2025-11-27T10:42:12",
    "status": "success",
    "duration_seconds": 111.52,
    "extract_rows": 0,
    "transform_rows": 186460,
    "load_success": true,
    "error": null
  }
}
```

## 🔧 Configuración

### Variables de Entorno (.env)

```env
# Base de datos
HOST_DBB=localhost
USER_DBB=usuario
PASSWORD_DBB=contraseña
NAME_DBB_DATALAKE_ECONOMICO=datalake_economico
NAME_DBB_DATALAKE_SOCIO=datalake_socio
NAME_DBB_DWH_SOCIO=dwh_socio

# Google Sheets
GOOGLE_SHEETS_API_KEY={"type": "service_account", ...}

# Logging
LOG_LEVEL=INFO
LOG_ROTATION=midnight
LOG_RETENTION_DAYS=30
```

## 🔄 Migración de Módulos Antiguos

Para migrar un módulo de `automaticos/scrap_*` a la nueva estructura:

1. **Analizar el módulo actual**: Identificar extract, transform, load
2. **Crear estructura nueva**: `modules/nuevo_nombre/`
3. **Migrar código**: Adaptar a clases base
4. **Probar**: Ejecutar y validar
5. **Documentar**: Agregar comentarios y docstrings

## 📈 Ventajas de la Nueva Estructura

1. **Escalabilidad**: Fácil agregar nuevos módulos
2. **Mantenibilidad**: Código organizado y reutilizable
3. **Testabilidad**: Componentes aislados y testeables
4. **Observabilidad**: Logs centralizados y registro de ejecuciones
5. **API Ready**: Base lista para panel web
6. **Producción Ready**: Scripts para EC2 y cron

## 🐛 Troubleshooting

### Error: "Módulo no encontrado"
- Verificar que el módulo existe en `modules/`
- Verificar que tiene `pipeline.py`

### Error: "Variables de entorno faltantes"
- Verificar archivo `.env` en la raíz
- Verificar que todas las variables requeridas están definidas

### Error: "No se puede conectar a BD"
- Verificar credenciales en `.env`
- Verificar que el servidor de BD está accesible

## 📚 Próximos Pasos

1. Migrar módulos restantes de `automaticos/` a `modules/`
2. Agregar tests unitarios para cada módulo
3. Implementar panel web completo con FastAPI
4. Agregar monitoreo y alertas
5. Documentar cada módulo individualmente




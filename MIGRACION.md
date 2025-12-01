# Guía de Migración - Estructura ETL Modular

## 📋 Resumen de Cambios

Este documento describe la reestructuración completa del proyecto ETL para hacerlo más escalable, mantenible y preparado para producción.

## 🎯 Objetivos de la Reestructuración

1. **Modularidad**: Cada ETL es un módulo independiente
2. **Reutilización**: Clases base comunes para todos los módulos
3. **Escalabilidad**: Fácil agregar nuevos módulos
4. **Producción**: Scripts para EC2 y cron jobs
5. **API Ready**: Base para panel web con FastAPI

## 📁 Cambios en la Estructura

### Antes (Estructura Antigua)

```
automaticos/
├── scrap_VentasCombustible/
│   ├── main.py
│   ├── extract.py
│   ├── transform.py
│   ├── conect_bdd.py
│   └── save_data_sheet.py
├── scrap_CanastaBasica/
│   ├── main.py
│   ├── etl/
│   └── extractors/
└── ...
```

### Después (Nueva Estructura)

```
├── config/              # ✨ NUEVO: Configuración centralizada
├── core/                # ✨ NUEVO: Clases base
├── modules/             # ✨ NUEVO: Módulos ETL organizados
│   └── ventas_combustible/
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── pipeline.py
├── utils/               # ✨ NUEVO: Utilidades centralizadas
├── scripts/             # ✨ NUEVO: Scripts para EC2
├── web/                 # ✨ NUEVO: API FastAPI
└── automaticos/         # ⚠️ LEGACY: Módulos antiguos (a migrar)
```

## 🔄 Plan de Migración

### Fase 1: Estructura Base ✅ COMPLETADA

- [x] Crear carpetas base (config/, core/, modules/, utils/, scripts/, web/)
- [x] Implementar clases base (BaseExtractor, BaseTransformer, BaseLoader)
- [x] Crear PipelineRunner
- [x] Implementar utils centralizados (logger, db, dates, mail)
- [x] Crear Settings centralizado

### Fase 2: Migración de Módulos

#### Módulo Ejemplo: VentasCombustible ✅ COMPLETADO

**Archivos movidos/renombrados:**

| Antes | Después | Cambios |
|-------|---------|---------|
| `automaticos/scrap_VentasCombustible/extract.py` | `modules/ventas_combustible/extract.py` | Hereda de `BaseExtractor` |
| `automaticos/scrap_VentasCombustible/transform.py` | `modules/ventas_combustible/transform.py` | Hereda de `BaseTransformer` |
| `automaticos/scrap_VentasCombustible/conect_bdd.py` | `modules/ventas_combustible/load.py` | Hereda de `BaseLoader`, usa `utils.db` |
| `automaticos/scrap_VentasCombustible/save_data_sheet.py` | Integrado en `load.py` | Lógica de Google Sheets en loader |
| `automaticos/scrap_VentasCombustible/main.py` | `modules/ventas_combustible/pipeline.py` | Usa `PipelineRunner` |

**Cambios principales:**

1. **Extractor**: Ahora hereda de `BaseExtractor` y retorna dict estandarizado
2. **Transformer**: Hereda de `BaseTransformer`, métodos tipados
3. **Loader**: Hereda de `BaseLoader`, usa `DatabaseConnection` de utils
4. **Pipeline**: Usa `PipelineRunner` para orquestación

#### Próximos Módulos a Migrar

1. **CanastaBasica** (Prioridad Alta)
   - Ya tiene estructura parcialmente organizada
   - Migrar a nueva estructura de módulos

2. **CBT** (Prioridad Media)
   - Módulo simple, fácil de migrar

3. **EMAE** (Prioridad Media)
   - Similar a VentasCombustible

4. **Otros módulos** (Prioridad Baja)
   - Migrar según necesidad

## 📝 Checklist de Migración por Módulo

Para migrar un módulo, seguir estos pasos:

### 1. Análisis
- [ ] Identificar componentes Extract, Transform, Load
- [ ] Identificar dependencias externas
- [ ] Identificar configuraciones específicas

### 2. Crear Estructura
- [ ] Crear carpeta `modules/nombre_modulo/`
- [ ] Crear `modules/nombre_modulo/files/` si es necesario
- [ ] Crear `__init__.py`

### 3. Migrar Extract
- [ ] Crear `extract.py` heredando de `BaseExtractor`
- [ ] Implementar método `extract()` tipado
- [ ] Mover lógica de extracción
- [ ] Usar logger centralizado

### 4. Migrar Transform
- [ ] Crear `transform.py` heredando de `BaseTransformer`
- [ ] Implementar método `transform()` tipado
- [ ] Mover lógica de transformación
- [ ] Validar retorno es DataFrame

### 5. Migrar Load
- [ ] Crear `load.py` heredando de `BaseLoader`
- [ ] Implementar método `load()` tipado
- [ ] Usar `DatabaseConnection` de utils
- [ ] Mover lógica de carga

### 6. Crear Pipeline
- [ ] Crear `pipeline.py`
- [ ] Instanciar componentes
- [ ] Usar `PipelineRunner`
- [ ] Configurar logging

### 7. Testing
- [ ] Probar ejecución completa
- [ ] Verificar logs
- [ ] Verificar carga en BD
- [ ] Verificar registro de ejecución

### 8. Documentación
- [ ] Agregar docstrings
- [ ] Documentar configuraciones específicas
- [ ] Actualizar README si es necesario

## 🔧 Cambios en el Código

### Antes: main.py directo

```python
# automaticos/scrap_VentasCombustible/main.py
from extract import Extraccion
from transform import Transformacion
from conect_bdd import ConexionBaseDatos

def main():
    extraccion = Extraccion()
    extraccion.descargar_archivo()
    
    transformacion = Transformacion()
    df = transformacion.crear_df()
    
    conexion = ConexionBaseDatos(...)
    conexion.main(df)
```

### Después: Pipeline con clases base

```python
# modules/ventas_combustible/pipeline.py
from core.pipeline_runner import PipelineRunner
from modules.ventas_combustible.extract import VentasCombustibleExtractor
from modules.ventas_combustible.transform import VentasCombustibleTransformer
from modules.ventas_combustible.load import VentasCombustibleLoader

def main():
    runner = PipelineRunner('ventas_combustible')
    
    extractor = VentasCombustibleExtractor()
    transformer = VentasCombustibleTransformer()
    loader = VentasCombustibleLoader()
    
    runner.set_components(extractor, transformer, loader)
    result = runner.run()
```

## 🚀 Ejecución

### Antes

```bash
cd automaticos/scrap_VentasCombustible
python main.py
```

### Después

```bash
# Desarrollo
python -m modules.ventas_combustible.pipeline

# Producción (EC2)
./scripts/run_module.sh ventas_combustible
```

## 📊 Beneficios de la Nueva Estructura

1. **Consistencia**: Todos los módulos siguen el mismo patrón
2. **Reutilización**: Código común en core/ y utils/
3. **Mantenibilidad**: Código organizado y fácil de encontrar
4. **Testabilidad**: Componentes aislados y testeables
5. **Observabilidad**: Logs centralizados y registro de ejecuciones
6. **Escalabilidad**: Fácil agregar nuevos módulos
7. **Producción**: Scripts listos para EC2 y cron

## ⚠️ Consideraciones

### Compatibilidad

- Los módulos antiguos en `automaticos/` siguen funcionando
- La migración es gradual, no requiere migrar todo de una vez
- Se puede ejecutar módulos antiguos y nuevos en paralelo

### Variables de Entorno

- Todas las variables de entorno se cargan desde `.env` en la raíz
- Settings centralizado valida variables requeridas
- Mismo formato de `.env` que antes

### Logs

- Logs antiguos: `logs/nombre_modulo.log`
- Logs nuevos: `logs/nombre_modulo.log` (mismo formato)
- Historial de ejecuciones: `logs/execution_history.json` (nuevo)

## 📚 Recursos

- **README_ESTRUCTURA.md**: Documentación completa de la estructura
- **Ejemplo migrado**: `modules/ventas_combustible/`
- **Clases base**: `core/`
- **Utils**: `utils/`

## 🎯 Próximos Pasos

1. Migrar módulos restantes según prioridad
2. Agregar tests unitarios
3. Implementar panel web completo
4. Agregar monitoreo y alertas
5. Optimizar performance

## ❓ Preguntas Frecuentes

**P: ¿Debo migrar todos los módulos de una vez?**
R: No, la migración es gradual. Puedes migrar módulo por módulo.

**P: ¿Los módulos antiguos seguirán funcionando?**
R: Sí, los módulos en `automaticos/` siguen funcionando normalmente.

**P: ¿Cómo ejecuto un módulo migrado?**
R: `python -m modules.nombre_modulo.pipeline`

**P: ¿Dónde están los logs?**
R: En `logs/nombre_modulo.log`, igual que antes.

**P: ¿Cómo agrego un nuevo módulo?**
R: Sigue la guía en `README_ESTRUCTURA.md` sección "Crear un Nuevo Módulo ETL".




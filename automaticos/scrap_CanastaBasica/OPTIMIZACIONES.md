# Análisis y Optimizaciones del Pipeline de Canasta Básica

## Problemas Identificados

### 1. **Cookies Desorganizadas** 🔴 CRÍTICO
- **Problema**: Las cookies se guardan en diferentes ubicaciones:
  - `carrefour_cookies.pkl` en raíz y en `files/`
  - `masonline_cookies.pkl` en raíz
  - `dia_cookies.pkl` en raíz
- **Impacto**: Dificulta el mantenimiento y puede causar conflictos
- **Solución**: Centralizar en `cookies/` usando `CookieManager`

### 2. **Procesamiento Secuencial** 🔴 CRÍTICO
- **Problema**: 
  - Supermercados se procesan uno por uno
  - Productos dentro de cada supermercado se procesan uno por uno
  - No hay paralelización
- **Impacto**: Tiempo total = suma de todos los tiempos individuales
- **Solución**: Procesamiento paralelo con `ThreadPoolExecutor`

### 3. **Tiempos de Espera Excesivos** 🟡 ALTO
- **Problema**:
  - Múltiples `time.sleep(2)`, `time.sleep(3)`, `time.sleep(5)` fijos
  - Esperas innecesarias después de cargar páginas
  - No se usan esperas inteligentes (WebDriverWait)
- **Impacto**: Añade minutos/horas innecesarias al proceso
- **Solución**: 
  - Reducir sleeps a mínimos necesarios (0.5s)
  - Usar `WebDriverWait` con timeouts cortos
  - Implementar `SmartWait` para esperas inteligentes

### 4. **Manejo de Sesiones Ineficiente** 🟡 ALTO
- **Problema**:
  - Cada extractor maneja sesiones de forma diferente
  - Algunos guardan cookies, otros no
  - No hay reutilización de sesiones entre productos
  - Se reinician drivers innecesariamente
- **Impacto**: Tiempo perdido en logins repetidos
- **Solución**: 
  - Centralizar manejo de cookies
  - Reutilizar drivers durante toda la sesión
  - Guardar sesiones solo al final

### 5. **Falta de Caché** 🟢 MEDIO
- **Problema**: No hay caché de resultados, se re-extraen productos ya procesados
- **Impacto**: Tiempo perdido en re-extracciones
- **Solución**: Implementar `ResultCache` con TTL configurable

### 6. **Configuración de Driver No Optimizada** 🟢 MEDIO
- **Problema**: Drivers no están optimizados para velocidad
- **Impacto**: Cargas de página más lentas
- **Solución**: Usar `page_load_strategy='eager'` y bloquear recursos innecesarios

## Optimizaciones Implementadas

### ✅ 1. CookieManager (`utils/cookie_manager.py`)
- Centraliza todas las cookies en `cookies/`
- Métodos: `save_cookies()`, `load_cookies()`, `delete_cookies()`
- Migración automática de cookies antiguas

### ✅ 2. Módulo de Optimización (`utils/optimization.py`)
- `SmartWait`: Esperas inteligentes
- `ResultCache`: Caché de resultados
- `ParallelProcessor`: Procesamiento paralelo
- `optimize_driver_options()`: Optimización de drivers

## Optimizaciones Pendientes

### 🔄 3. Actualizar Extractores para usar CookieManager
- Modificar `CarrefourExtractor` para usar `CookieManager`
- Modificar `MasonlineExtractor` para usar `CookieManager`
- Actualizar otros extractores si usan cookies

### 🔄 4. Implementar Procesamiento Paralelo en `extract.py`
- Procesar supermercados en paralelo (3-5 workers)
- Procesar productos en lotes paralelos dentro de cada supermercado

### 🔄 5. Reducir Tiempos de Espera
- Reemplazar `time.sleep()` fijos por `SmartWait`
- Reducir timeouts de 30s a 15s
- Usar `page_load_strategy='eager'`

### 🔄 6. Optimizar Flujo de Extracción
- Inicializar sesiones solo una vez al inicio
- Reutilizar drivers durante toda la ejecución
- Guardar cookies solo al finalizar

## Estimación de Mejoras

### Tiempo Actual: ~5 horas
- Procesamiento secuencial: ~4.5 horas
- Tiempos de espera innecesarios: ~0.5 horas

### Tiempo Esperado Después de Optimizaciones: ~1-1.5 horas
- **Procesamiento paralelo (3 workers)**: Reducción ~60% = 1.8 horas
- **Reducción de esperas**: Reducción ~40% = 0.3 horas
- **Caché y optimizaciones**: Reducción ~20% = 0.2 horas
- **Total estimado**: ~1.3 horas

### Mejora Total: **~70% de reducción de tiempo**

## Plan de Implementación

1. ✅ Crear `CookieManager` - COMPLETADO
2. ✅ Crear módulo de optimización - COMPLETADO
3. 🔄 Actualizar extractores para usar `CookieManager`
4. 🔄 Implementar procesamiento paralelo en `extract.py`
5. 🔄 Reducir tiempos de espera en extractores
6. 🔄 Optimizar configuración de drivers
7. 🔄 Implementar caché de resultados

## Notas Importantes

- **Paralelización**: No paralelizar demasiado (máx 3-5 workers) para evitar bloqueos por parte de los sitios
- **Cookies**: Migrar cookies antiguas antes de eliminar archivos
- **Testing**: Probar cada optimización individualmente antes de aplicar todas juntas
- **Logging**: Mantener logging detallado para monitorear mejoras



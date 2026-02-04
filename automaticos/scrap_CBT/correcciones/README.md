# Correcciones Manuales

Esta carpeta está destinada para scripts de corrección manual de datos.

## Uso

1. Crear un script de corrección con fecha: `correccion_YYYY_MM_DD.py`
2. Documentar el problema y la solución en el script
3. Ejecutar manualmente cuando sea necesario
4. Mantener un registro de las correcciones aplicadas

## Ejemplo

```python
# correccion_2025_01_15.py
# Problema: Valores duplicados en enero 2025
# Solución: Eliminar duplicados y recalcular estimaciones

import pandas as pd
from load.database_loader import DatabaseLoader

# Tu código de corrección aquí
```

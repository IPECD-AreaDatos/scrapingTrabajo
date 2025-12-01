# Generador de Reporte por Categoría

Script para generar reportes Excel de productos agrupados por categorías.

## Uso

### Opción 1: Usar último CSV generado
```bash
python generate_report_by_category.py --ultimo
```

### Opción 2: Especificar archivo CSV
```bash
python generate_report_by_category.py --csv canasta_basica_completa_20251126_1430.csv
```

### Opción 3: Sin argumentos (busca último CSV automáticamente)
```bash
python generate_report_by_category.py
```

## Estructura del Excel Generado

El reporte incluye las siguientes hojas:

1. **Resumen**: Estadísticas por categoría
   - Productos únicos por categoría
   - Total de registros
   - Supermercados disponibles
   - Precio promedio

2. **Todos los Productos**: Lista completa de todos los productos con sus categorías

3. **Hojas por Categoría**: Una hoja por cada categoría encontrada
   - Ejemplo: "Aceites y Grasas", "Lácteos", "Panadería", etc.

## Configuración de Categorías

Las categorías se definen en `config/categorias.py`:

### Agregar productos nuevos

1. **Match exacto**: Agregar el nombre exacto del producto en `CATEGORIAS_PRODUCTOS`
```python
CATEGORIAS_PRODUCTOS = {
    'Aceite de Girasol': 'Aceites y Grasas',
    'Leche Entera': 'Lácteos',
    # ... más productos
}
```

2. **Palabras clave**: Agregar palabras clave en `PALABRAS_CLAVE_CATEGORIAS`
```python
PALABRAS_CLAVE_CATEGORIAS = {
    'Aceites y Grasas': ['aceite', 'grasa', 'manteca'],
    'Lácteos': ['leche', 'yogur', 'queso'],
    # ... más categorías
}
```

## Categorías Disponibles

- Aceites y Grasas
- Lácteos
- Panadería
- Almacén
- Carnes
- Frutas y Verduras
- Bebidas
- Limpieza
- Higiene Personal
- Sin Categoría (productos no clasificados)

## Requisitos

- `pandas`
- `openpyxl` (ya incluido en requirements.txt)

## Ejemplo de Salida

```
productos_por_categoria_20251127_0136.xlsx
├── Resumen
├── Todos los Productos
├── Aceites y Grasas
├── Lácteos
├── Panadería
└── ...
```







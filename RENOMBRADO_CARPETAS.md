# 📝 Renombrado de Carpetas - camelCase

## ✅ Cambios Realizados

Se renombraron las carpetas para seguir una convención camelCase consistente:

### Carpetas Renombradas:

1. ✅ `scrap_canasta_basica` → `scrap_CanastaBasica`
2. ✅ `scrap_semaforo` → `scrap_Semaforo`
3. ✅ `scrap_ventas_cobustible` → `scrap_VentasCombustible` (typo corregido: "cobustible" → "Combustible")
4. ✅ `scrap_SalarioSP-Total` → `scrap_SalarioSPTotal` (guión eliminado)
5. ✅ `scrap_Indice_Salarios` → `scrap_IndiceSalarios`

### Carpetas que ya estaban en camelCase:

- `scrap_PuestosTrabajoSP` ✓
- `scrap_SalarioMVM` ✓
- `scrap_Supermercados` ✓

### Carpetas con guiones bajos (mantenidas por ser variantes):

- `scrap_IPC_CABA` (variante de IPC)
- `scrap_IPC_Online` (variante de IPC)

### Carpetas con siglas (mantenidas en mayúsculas):

- `scrap_ANAC`, `scrap_CBT`, `scrap_DNRPA`, `scrap_DOLAR`, `scrap_EMAE`, `scrap_IERIC`, `scrap_IPC`, `scrap_IPI`, `scrap_IPICORR`, `scrap_OEDE`, `scrap_REM`, `scrap_RIPTE`, `scrap_SIPA`, `scrap_SRT`

---

## 📋 Estructura Final (24 scrapers)

```
automaticos/
├── scrap_ANAC
├── scrap_CanastaBasica          ← Renombrado
├── scrap_CBT
├── scrap_DNRPA
├── scrap_DOLAR
├── scrap_EMAE
├── scrap_IERIC
├── scrap_IndiceSalarios         ← Renombrado
├── scrap_IPC
├── scrap_IPC_CABA
├── scrap_IPC_Online
├── scrap_IPI
├── scrap_IPICORR
├── scrap_OEDE
├── scrap_PuestosTrabajoSP
├── scrap_REM
├── scrap_RIPTE
├── scrap_SalarioMVM
├── scrap_SalarioSPTotal         ← Renombrado
├── scrap_Semaforo               ← Renombrado
├── scrap_SIPA
├── scrap_SRT
├── scrap_Supermercados
└── scrap_VentasCombustible      ← Renombrado (typo corregido)
```

---

## 🔄 Archivos Actualizados

- ✅ `scripts/execute_automaticos.sh` - Rutas actualizadas con los nuevos nombres

---

**Fecha:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")









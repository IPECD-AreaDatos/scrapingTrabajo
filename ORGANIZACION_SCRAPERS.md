# 📁 Organización de Scrapers

## ✅ Estado: COMPLETADO

Los scrapers han sido organizados en dos carpetas principales según su tipo de ejecución.

---

## 🟢 AUTOMÁTICOS (24 scrapers)
**Ubicación:** `automaticos/`  
**Uso:** Para servidor EC2 - Ejecución automática programada

1. `scrap_ANAC` - Administración Nacional de Aviación Civil
2. `scrap_canasta_basica` - Canasta Básica (Supermercados)
3. `scrap_CBT` - Canasta Básica y Total
4. `scrap_DNRPA` - Registro Automotor
5. `scrap_DOLAR` - Tipos de cambio (Oficial, Blue, MEP, CCL)
6. `scrap_EMAE` - Estimador Mensual de Actividad Económica
7. `scrap_IERIC` - Instituto de Estadística y Registro de la Industria de la Construcción
8. `scrap_Indice_Salarios` - Índice de Salarios
9. `scrap_IPC` - Índice de Precios al Consumidor
10. `scrap_IPC_CABA` - IPC CABA
11. `scrap_IPC_Online` - IPC Pronosticado
12. `scrap_IPI` - Índice de Producción Industrial
13. `scrap_IPICORR` - IPI en Corrientes
14. `scrap_OEDE` - (Revisar propósito)
15. `scrap_PuestosTrabajoSP` - Puestos de Trabajo Sector Privado
16. `scrap_REM` - Expectativas de Inflación BCRA
17. `scrap_RIPTE` - Remuneración Imponible Promedio
18. `scrap_SalarioMVM` - Salario Mínimo Vital y Móvil
19. `scrap_SalarioSP-Total` - Salarios Sector Privado y Total
20. `scrap_semaforo` - Semáforo de Indicadores
21. `scrap_SIPA` - Sistema Integrado Previsional Argentino
22. `scrap_SRT` - Superintendencia de Riesgos del Trabajo
23. `scrap_Supermercados` - Datos de Supermercados
24. `scrap_ventas_cobustible` - Ventas de Combustible

---

## 🟡 MANUALES (15 scrapers)
**Ubicación:** `manuales/`  
**Uso:** Ejecución local bajo demanda

1. `carga_diccionario_clae` - Diccionario CLAE (carga desde Google Sheets)
2. `carga_PBG` - Producto Bruto Geográfico (carga desde Google Sheets)
3. `carga_rama_actividad_economica` - Rama de Actividad Económica (requiere archivo Excel)
4. `scrap_Censo_IPECD` - Censo IPECD (carga desde Google Sheets)
5. `scrap_Censo_IPECD_Jose` - Censo IPECD - Variante (múltiples cargas)
6. `scrap_Censo_Municipio` - Censo por Municipio (carga desde Google Sheets)
7. `scrap_Censos` - Censo General (datos históricos)
8. `scrap_ECV` - Encuesta de Calidad de Vida (menú interactivo)
9. `scrap_EPH` - Encuesta Permanente de Hogares
10. `scrap_IPC_tabla` - IPC Tabla (revisar propósito)
11. `scrap_nacion_nea_Jose` - Censo Nación NEA (requiere archivo Excel)
12. `scrap_PBG` - PBG (revisar diferencia con carga_PBG)
13. `scrap_ReconocimientoMedicos` - Reconocimiento Médicos
14. `scrap_SISPER` - SISPER
15. `script_PuestosCadaMilHabitantes` - Cálculo de Puestos por Mil Habitantes

---

## 📝 Notas

- **scrap_OEDE**: Está en automáticos pero necesita revisión de propósito
- **scrap_EPH**: Está en manuales pero podría ser automático si se actualiza regularmente
- **scrap_IPC_tabla**: Necesita revisión de propósito
- **scrap_PBG vs carga_PBG**: Verificar si son diferentes o duplicados

---

## 🚀 Próximos Pasos

1. ✅ Organización en carpetas completada
2. ⏳ Actualizar scripts de ejecución (`execute_small_batch.sh`, etc.)
3. ⏳ Crear script de ejecución para automáticos
4. ⏳ Crear script de ejecución para manuales
5. ⏳ Actualizar documentación

---

**Última actualización:** $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")


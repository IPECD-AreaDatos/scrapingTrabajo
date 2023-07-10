USE prueba1;

-- Trabajo N°1: Carga de datos de Puestos de trabajo asalariados del sector privado
Select * from Provincias;
Select * from Departamentos;

-- Tabla con los puestos de trabajo  
Select COUNT(*)  from puestos_trabajo_asalariado;
Select * from puestos_trabajo_asalariado limit 3000;
SELECT * FROM puestos_trabajo_asalariado WHERE codigo_departamento_indec IS NULL;

-- Union Provincias y Departamentos
SELECT provincias.nombre_provincia_indec AS provincia, localidades.nombre_departamento_indec AS localidad, localidades.codigo_departamento_indec AS codigo_departamento_indec
FROM localidades
JOIN provincias ON localidades.id_provincia_indec = provincias.id_provincia_indec;
-- Union de tablas // Tabla Final
SELECT provincias.nombre_provincia_indec AS provincia, 
       localidades.nombre_departamento_indec AS localidad, 
       puestos_trabajo_asalariado.fecha AS fecha,
       sectores_de_actividad.clae2_desc AS sector_de_actividad,
       puestos_trabajo_asalariado.puestos AS puestos
FROM localidades
JOIN provincias ON localidades.id_provincia_indec = provincias.id_provincia_indec
JOIN puestos_trabajo_asalariado ON localidades.codigo_departamento_indec = puestos_trabajo_asalariado.codigo_departamento_indec
JOIN sectores_de_actividad ON puestos_trabajo_asalariado.clae2 = sectores_de_actividad.clae2;

-- Trabajo2: Tabla de IPC(Indice de Precio al Consumidor)
-- Ver los datos de las tablas de IPC
-- Nacional
SELECT * FROM prueba1.ipc_totalnacion;
-- Regiones
SELECT * FROM prueba1.ipc_regionnea;
SELECT * FROM prueba1.ipc_regiongba;
SELECT * FROM prueba1.ipc_regionpampeana;
SELECT * FROM prueba1.ipc_regionnoroeste;
SELECT * FROM prueba1.ipc_regioncuyo;
SELECT * FROM prueba1.ipc_regionpatagonia;

-- Ver los datos de las varianzas
-- Ver los datos de las tablas de varianza intermensual
-- Nacional
SELECT * FROM prueba1.variacion_intermensual_nacion;
-- Regiones
SELECT Fecha, CONCAT(FORMAT(Nivel_General * 100, 3), '%') AS porcentaje_formatado
FROM variacion_intermensual_nacion;

SELECT * FROM prueba1.variacion_intermensual_cuyo;
SELECT * FROM prueba1.variacion_intermensual_gba;
SELECT * FROM prueba1.variacion_intermensual_noroeste;
SELECT * FROM prueba1.variacion_intermensual_pampeana;
SELECT * FROM prueba1.variacion_intermensual_patagonia;
SELECT * FROM prueba1.variacion_intermensual_nea;

-- Variacion Interanual
-- Ver los datos de las tablas de varianza interanual
-- Nacional
SELECT * FROM prueba1.variacion_interanual_nacion;
-- Regiones
SELECT * FROM prueba1.variacion_interanual_cuyo;
SELECT * FROM prueba1.variacion_interanual_gba;
SELECT * FROM prueba1.variacion_interanual_noroeste;
SELECT * FROM prueba1.variacion_interanual_pampeana;
SELECT * FROM prueba1.variacion_interanual_patagonia;
SELECT * FROM prueba1.variacion_interanual_nea;

-- SIPA
SELECT * FROM prueba1.sipa_nacional_con_estacionalidad;
SELECT * FROM prueba1.sipa_nacional_sin_estacionalidad;
SELECT * FROM prueba1.sipa_provincia_con_estacionalidad;
SELECT * FROM prueba1.sipa_provincia_sin_estacionalidad;

-- DNRPA 
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_moto;
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_moto;
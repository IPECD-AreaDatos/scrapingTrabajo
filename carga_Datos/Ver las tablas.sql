USE prueba1;

-- Trabajo N°1: Carga de datos de Puestos de trabajo asalariados del sector privado
Select * from dp_provincias;
Select * from Departamentos;

-- Tabla con los puestos de trabajo  
Select COUNT(*)  from dp_puestostrabajo_sector_privado;
Select * from dp_puestostrabajo_sector_privado limit 3000;
SELECT * FROM dp_puestostrabajo_sector_privado WHERE codigo_departamento_indec IS NULL;

-- Union dp_provincias y Departamentos
SELECT dp_provincias.nombre_provincia_indec AS dp_provincias, dp_localidades.nombre_departamento_indec AS dp_localidad, dp_localidades.codigo_departamento_indec AS codigo_departamento_indec
FROM dp_localidades
JOIN dp_provincias ON dp_localidades.id_provincia_indec = dp_provincias.id_provincia_indec;
-- Union de tablas // Tabla Final
SELECT dp_provincias.nombre_provincia_indec AS provincias, 
       dp_localidades.nombre_departamento_indec AS localidad, 
       dp_puestostrabajo_sector_privado.fecha AS fecha,
       dp_sectores_de_actividad.clae2_desc AS sector_de_actividad,
       dp_puestostrabajo_sector_privado.puestos AS puestos
FROM dp_localidades
JOIN dp_provincias ON dp_localidades.id_provincia_indec = dp_provincias.id_provincia_indec
JOIN dp_puestostrabajo_sector_privado ON dp_localidades.codigo_departamento_indec = dp_puestostrabajo_sector_privado.codigo_departamento_indec
JOIN dp_sectores_de_actividad ON dp_puestostrabajo_sector_privado.clae2 = dp_sectores_de_actividad.clae2;

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
SELECT * FROM prueba1.sipa_dp_provincias_con_estacionalidad;
SELECT * FROM prueba1.sipa_dp_provincias_sin_estacionalidad;

-- DNRPA 
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_moto;
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_moto;
SELECT * FROM prueba1.dnrpa_parque_activo_nacion;

-- SALARIOS
SELECT * FROM dp_salarios_sector_privado WHERE MONTH(fecha) = 10 AND YEAR(fecha) = 2022;
SELECT dp_provincias.nombre_provincia_indec AS provincias, 
       dp_localidades.nombre_departamento_indec AS localidad, 
       dp_salarios_sector_privado.fecha AS fecha,
       dp_sectores_de_actividad.clae2_desc AS sector_de_actividad,
       dp_salarios_sector_privado.salario AS salario
FROM dp_localidades
JOIN dp_provincias ON dp_localidades.id_provincia_indec = dp_provincias.id_provincia_indec
JOIN dp_salarios_sector_privado ON dp_localidades.codigo_departamento_indec = dp_salarios_sector_privado.codigo_departamento_indec
JOIN dp_sectores_de_actividad ON dp_salarios_sector_privado.clae2 = dp_sectores_de_actividad.clae2
WHERE YEAR(dp_salarios_sector_privado.fecha) = 2023 AND dp_provincias.nombre_provincia_indec = 'Corrientes';
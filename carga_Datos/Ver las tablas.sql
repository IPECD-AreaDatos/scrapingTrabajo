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
SELECT ipc_region.Fecha, regiones.descripcion_region AS Region, subdivision.descripcion AS Subdivision, ipc_region.Valor
FROM ipc_region
INNER JOIN regiones ON ipc_region.ID_Region = regiones.id_region
INNER JOIN subdivision ON ipc_region.ID_Subdivision = subdivision.id_subdivision
LIMIT 30000;
Select COUNT(*)  from ipc_region;

-- SIPA
SELECT sipa_registro.Fecha, dp_provincias.nombre_provincia_indec AS Provincia, sipa_tiporegistro.Descripcion_Registro AS Registro, sipa_registro.Cantidad_con_Estacionalidad, sipa_registro.Cantidad_sin_Estacionalidad
FROM sipa_registro
INNER JOIN dp_provincias ON sipa_registro.ID_Provincia = dp_provincias.id_provincia_indec
INNER JOIN sipa_tiporegistro ON sipa_registro.ID_Tipo_Registro = sipa_tiporegistro.ID_Registro;

-- DNRPA 
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_moto;
SELECT * FROM prueba1.dnrpa_inscripcion_corrientes_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_auto;
SELECT * FROM prueba1.dnrpa_inscripcion_nacion_moto;
SELECT * FROM prueba1.dnrpa_parque_activo_nacion;

-- SALARIOS dp=Desarrollo Productivo
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
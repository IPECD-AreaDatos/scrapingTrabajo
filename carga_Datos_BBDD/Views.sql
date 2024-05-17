USE prueba1;

CREATE VIEW vista_censo AS
SELECT 
    cp.Fecha as Fecha,
    dp_provincias.nombre_provincia_indec as Provincia,
    dp_localidades.nombre_departamento_indec as Departamento,
    cp.Poblacion as Poblacion
FROM
    censo_provincia cp
JOIN
    dp_localidades ON cp.ID_Departamento = dp_localidades.codigo_departamento_indec
JOIN
    dp_provincias ON dp_localidades.id_provincia_indec = dp_provincias.id_provincia_indec;


create view vista_sipa as 
Select
	sipa_registro.Fecha as Fecha,
    dp_provincias.nombre_provincia_indec as Provincia,
    sipa_tiporegistro.Descripcion_Registro as Registro,
    sipa_registro.Cantidad_con_Estacionalidad as Cantidad_con_Estacionalidad,
    sipa_registro.Cantidad_sin_Estacionalidad as Cantidad_sin_Estacionalidad
From
	sipa_registro
join
	dp_provincias on sipa_registro.ID_Provincia=dp_provincias.id_provincia_indec
join 
	sipa_tiporegistro on sipa_registro.ID_Tipo_Registro=sipa_tiporegistro.ID_Registro;

create view vista_ipc_valores as
select
	ipc_valores.Fecha as Fecha,
    identificador_regiones.descripcion_region as Region,
	ipc_categoria.nombre as Categoria,
    ipc_division.nombre as Division,
    ipc_subdivision.nombre as Subdivision, 
    ipc_valores.Valor as Valor
FROM
	ipc_valores
join
	identificador_regiones on ipc_valores.ID_Region = identificador_regiones.ID_Region
join 
	ipc_categoria on ipc_valores.ID_Categoria = ipc_categoria.id_categoria
join
	ipc_division on ipc_valores.ID_Division = ipc_division.id_division
join
	ipc_subdivision on ipc_valores.ID_Subdivision = ipc_subdivision.id_subdivision;
    
CREATE VIEW Vista_Puestos_Trabajo_Sector_Privado as
SELECT dp_provincias.nombre_provincia_indec AS Provincia, 
       dp_localidades.nombre_departamento_indec AS Localidad, 
       dp_puestostrabajo_sector_privado.fecha AS Fecha,
       dp_sectores_de_actividad.clae2_desc AS Sector_de_actividad,
       dp_puestostrabajo_sector_privado.puestos AS Puestos
FROM dp_puestostrabajo_sector_privado
JOIN dp_localidades ON dp_localidades.codigo_departamento_indec = dp_puestostrabajo_sector_privado.codigo_departamento_indec
JOIN dp_provincias ON dp_provincias.id_provincia_indec = dp_localidades.id_provincia_indec
JOIN dp_sectores_de_actividad ON dp_sectores_de_actividad.clae2 = dp_puestostrabajo_sector_privado.clae2;
	
    
CREATE VIEW Vista_Puestos_Trabajo_Total as
SELECT dp_provincias.nombre_provincia_indec AS Provincia, 
       dp_localidades.nombre_departamento_indec AS Localidad, 
       dp_puestostrabajo_total.fecha AS Fecha,
       dp_sectores_de_actividad.clae2_desc AS Sector_de_actividad,
       dp_puestostrabajo_total.puestos AS Puestos
FROM dp_puestostrabajo_total
JOIN dp_localidades ON dp_localidades.codigo_departamento_indec = dp_puestostrabajo_total.codigo_departamento_indec
JOIN dp_provincias ON dp_provincias.id_provincia_indec = dp_localidades.id_provincia_indec
JOIN dp_sectores_de_actividad ON dp_sectores_de_actividad.clae2 = dp_puestostrabajo_total.clae2;

CREATE VIEW Vista_Salario_Puestos_Trabajo_Sector_Privado as
SELECT dp_provincias.nombre_provincia_indec AS Provincia, 
       dp_localidades.nombre_departamento_indec AS Localidad, 
       dp_salarios_sector_privado.fecha AS Fecha,
       dp_sectores_de_actividad.clae2_desc AS Sector_de_actividad,
       dp_salarios_sector_privado.salario AS Salario
FROM dp_salarios_sector_privado
JOIN dp_localidades ON dp_localidades.codigo_departamento_indec = dp_salarios_sector_privado.codigo_departamento_indec
JOIN dp_provincias ON dp_provincias.id_provincia_indec = dp_localidades.id_provincia_indec
JOIN dp_sectores_de_actividad ON dp_sectores_de_actividad.clae2 = dp_salarios_sector_privado.clae2;

CREATE VIEW Vista_Salario_Puestos_Trabajo_Total as
SELECT dp_provincias.nombre_provincia_indec as Provincia,
	dp_localidades.nombre_departamento_indec as Localidad,
	dp_salarios_total.fecha as Fecha,
    dp_sectores_de_actividad.clae2_desc as Sector_de_actividad,
    dp_salarios_total.salario as Salarios
From dp_salarios_total
join dp_localidades on dp_localidades.codigo_departamento_indec = dp_salarios_total.codigo_departamento_indec
join dp_provincias on dp_provincias.id_provincia_indec = dp_localidades.id_provincia_indec
join dp_sectores_de_actividad on dp_sectores_de_actividad.clae2 = dp_salarios_total.clae2;
    
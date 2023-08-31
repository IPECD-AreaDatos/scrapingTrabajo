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
    
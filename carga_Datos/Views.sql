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
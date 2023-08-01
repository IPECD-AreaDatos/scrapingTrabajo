USE prueba1;
-- Diccionario Regiones
CREATE TABLE regiones (
ID_Region INT PRIMARY KEY NOT NULL,
descripcion_region varchar(30)
);

INSERT INTO regiones(id_region , descripcion_region) Values (1, "Nacion");
INSERT INTO regiones(id_region , descripcion_region) Values (2, "GBA");
INSERT INTO regiones(id_region , descripcion_region) Values (3, "Pampeana");
INSERT INTO regiones(id_region , descripcion_region) Values (4, "NOA");
INSERT INTO regiones(id_region , descripcion_region) Values (5, "NEA");
INSERT INTO regiones(id_region , descripcion_region) Values (6, "Cuyo");
INSERT INTO regiones(id_region , descripcion_region) Values (7, "Patagonia");


-- Tabla de dp_provincias
CREATE TABLE dp_provincias(
id_provincia_indec int primary key not null,
nombre_provincia_indec varchar(20)
);

Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (2, 'CABA');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (6, 'Buenos Aires');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (10, 'Catamarca');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (14, 'Cordoba');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (18, 'Corrientes');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (22, 'Chaco');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (26, 'Chubut');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (30, 'Entre Rios');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (34, 'Formosa');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (38, 'Jujuy');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (42, 'La Pampa');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (46, 'La Rioja');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (50, 'Mendoza');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (54, 'Misiones');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (58, 'Neuquen');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (62, 'Rio Negro');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (66, 'Salta');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (70, 'San Juan');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (74, 'San Luis');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (78, 'Santa Cruz');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (82, 'Santa Fe');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (86, 'Santiago Del Estero');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (90, 'Tucuman');
Insert into dp_provincias(id_provincia_indec, nombre_provincia_indec) Values (94, 'Tierra Del Fuego');


-- Tabla de dp_localidades
CREATE TABLE Departamentos (
  codigo_departamento_indec INTEGER,
  nombre_departamento_indec VARCHAR(255),
  id_provincia_indec INTEGER,
  nombre_provincia_indec VARCHAR(255)
);

-- Tabla con los puestos de trabajo  
CREATE TABLE puestos_trabajo_asalariado (
  fecha Date,
  codigo_departamento_indec INT,
  id_provincia_indec INT,
  clae2 VARCHAR(255),
  puestos INT
);

-- Tabla de clae2(Sectores de actividad)
CREATE TABLE dp_sectores_de_actividad (
  clae2 INT PRIMARY KEY,
  clae2_desc VARCHAR(255)
);

INSERT INTO dp_sectores_de_actividad (clae2, clae2_desc)
VALUES
(1, 'Agricultura, ganadería, caza y servicios relacionados'),
(2, 'Silvicultura y explotación forestal'),
(3, 'Pesca y acuicultura'),
(5, 'Extracción de carbón y lignito'),
(6, 'Extracción de petróleo crudo y gas natural'),
(7, 'Extracción de minerales metálicos'),
(8, 'Extracción de otros minerales'),
(9, 'Actividades de apoyo al petróleo y la minería'),
(10, 'Elaboración de productos alimenticios'),
(11, 'Elaboración de bebidas'),
(12, 'Elaboración de productos de tabaco'),
(13, 'Elaboración de productos textiles'),
(14, 'Elaboración de prendas de vestir'),
(15, 'Elaboración de productos de cuero y calzado'),
(16, 'Elaboración de productos de madera'),
(17, 'Elaboración de productos de papel'),
(18, 'Imprentas y editoriales'),
(19, 'Fabricación de productos de refinación de petróleo'),
(20, 'Fabricación de sustancias químicas'),
(21, 'Elaboracion de productos farmacéuticos'),
(22, 'Fabricación de productos de caucho y vidrio'),
(23, 'Fabricación de productos de vidrio y otros minerales no metálicos'),
(24, 'Fabricación de metales comunes'),
(25, 'Fabricación de productos elaborados del metal, excepto maquinaria y equipo'),
(26, 'Fabricación de productos de informática, de electrónica y de óptica'),
(27, 'Fabricación de equipo eléctrico'),
(28, 'Fabricación de maquinarias'),
(29, 'Fabricación de vehículos automotores, remolques y semirremolques'),
(30, 'Fabricación de otros equipos de transporte'),
(31, 'Fabricación de muebles'),
(32, 'Otras industrias manufactureras'),
(33, 'Reparación e instalación de maquinaria y equipo'),
(35, 'Suministro de electricidad, gas, vapor y aire acondicionado'),
(36, 'Captación, tratamiento y distribución de agua'),
(37, 'Evacuación de aguas residuales'),
(38, 'Recogida, tratamiento y eliminación de desechos'),
(39, 'Descontaminación y otros servicios'),
(41, 'Construcción de edificios y partes'),
(42, 'Obras de ingeniería civil'),
(43, 'Actividades especializadas de construcción'),
(45, 'Comercio al por mayor y al por menor y reparación de vehículos automotores y motos'),
(46, 'Comercio al por mayor excepto autos y motos'),
(47, 'Comercio al por menor excepto autos y motos'),
(49, 'Transporte terrestre y por tuberías'),
(50, 'Transporte acuático'),
(51, 'Transporte aéreo'),
(52, 'Almacenamiento y actividades de apoyo al transporte'),
(53, 'Servicio de correo y mensajería'),
(55, 'Servicios de alojamiento'),
(56, 'Servicios de expendio de alimentos y bebidas'),
(58, 'Edición'),
(59, 'Servicios audiovisuales'),
(60, 'Programación y transmisiones de TV y radio'),
(61, 'Telecomunicaciones'),
(62, 'Servicios de programación, consultoría informática y actividades conexas'),
(63, 'Actividades de procesamiento de información'),
(64, 'Servicios financieros (excepto seguros y pensiones)'),
(65, 'Servicios de seguros, reaseguros y pensiones'),
(66, 'Servicios auxiliares de la actividad financiera y de seguros'),
(68, 'Servicios inmobiliarios'),
(69, 'Servicios jurídicos y de contabilidad'),
(70, 'Servicios y asesoramiento de dirección y gestión empresarial'),
(71, 'Servicios de arquitectura e ingeniería'),
(72, 'Investigación y desarrollo científico'),
(73, 'Servicios de publicidad e investigación de mercado'),
(74, 'Otras actividades profesionales, científicas y técnicas'),
(75, 'Servicios veterinarios'),
(77, 'Alquiler y arrendamiento'),
(78, 'Servicios de obtención y dotación de personal'),
(79, 'Agencias de viajes, servicios de reservas y actividades conexas'),
(80, 'Actividades de investigación y seguridad'),
(81, 'Servicios a edificios y actividades de jardinería'),
(82, 'Actividades administrativas y de apoyo a oficinas y empresas'),
(84, 'Administración pública, defensa y seguridad social'),
(85, 'Enseñanza'),
(86, 'Servicios de salud humana'),
(87, 'Actividades de atención en instituciones'),
(88, 'Actividades de atención sin alojamiento'),
(90, 'Servicios artísticos y de espectáculos'),
(91, 'Servicios de bibliotecas, archivos y museos y servicios culturales n.c.p.'),
(92, 'Juegos de azar y apuestas'),
(93, 'Actividades deportivas, recreativas y de entretenimiento'),
(94, 'Servicios de asociaciones'),
(95, 'Reparación de computadoras y equipos de uso doméstico'),
(96, 'Otros servicios personales'),
(999, 'Otros sectores');


-- Tabla de IPC
CREATE TABLE ipc_region(
Fecha date not null,
ID_Region int,
ID_Subdivision int,
Valor float,

Foreign key (ID_Region) references regiones(id_region),
Foreign key (ID_Subdivision) references subdivision(id_subdivision)
);

-- TABLAS SIPA
CREATE TABLE sipa_registro(
Fecha date not null,
ID_Provincia int,
ID_Tipo_Registro int,
Cantidad_con_Estacionalidad float,
Cantidad_sin_Estacionalidad float,

Foreign key (ID_Provincia) references dp_provincias(id_provincia_indec),
Foreign key (ID_Tipo_Registro) references sipa_tiporegistro(ID_Registro)
);

CREATE TABLE DP_salarios_sector_privado(
  fecha Date,
  codigo_departamento_indec INT,
  id_provincia_indec INT,
  clae2 VARCHAR(255),
  salario INT
);
CREATE TABLE DP_salarios_total(
  fecha Date,
  codigo_departamento_indec INT,
  id_provincia_indec INT,
  clae2 VARCHAR(255),
  salario INT
);

Create table censo_provincia(
Fecha date not null,
ID_Provincia int,
ID_Departamentos int,
Poblacion int,

Foreign key (ID_Provincia) references dp_provincias(id_provincia_indec)
);

Create Table dnrpa_inscripcion(
Fecha date not null,
ID_Vehiculo int,
ID_Provincia int, 
Region varchar(125),
Cantidad int,

Foreign key (ID_Provincia) references dp_provincias(id_provincia_indec),
Foreign key (ID_Vehiculo) references dnrpa_vehiculos(ID_Vehiculo)
);

Create Table dnrpa_vehiculos(
ID_Vehiculo int PRIMARY KEY, 
Tipo_Vehiculo char(20)
);
INSERT INTO dnrpa_vehiculos (ID_Vehiculo, Tipo_Vehiculo)
VALUES
(1, 'Auto'),
(2, 'Moto');
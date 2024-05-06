USE prueba1;
-- Diccionario Regiones
CREATE TABLE identificador_regiones (
ID_Region INT PRIMARY KEY NOT NULL,
descripcion_region varchar(30)
);

INSERT INTO identificador_regiones(id_region , descripcion_region) Values (1, "Nacion");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (2, "GBA");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (3, "Pampeana");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (4, "NOA");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (5, "NEA");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (6, "Cuyo");
INSERT INTO identificador_regiones(id_region , descripcion_region) Values (7, "Patagonia");


-- Tabla de identificador_provincias
CREATE TABLE identificador_provincias(
id_provincia_indec int primary key not null,
nombre_provincia_indec varchar(20)
);
insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) values (1, 'Nacion');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (2, 'CABA');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (6, 'Buenos Aires');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (10, 'Catamarca');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (14, 'Cordoba');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (18, 'Corrientes');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (22, 'Chaco');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (26, 'Chubut');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (30, 'Entre Rios');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (34, 'Formosa');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (38, 'Jujuy');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (42, 'La Pampa');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (46, 'La Rioja');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (50, 'Mendoza');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (54, 'Misiones');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (58, 'Neuquen');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (62, 'Rio Negro');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (66, 'Salta');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (70, 'San Juan');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (74, 'San Luis');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (78, 'Santa Cruz');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (82, 'Santa Fe');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (86, 'Santiago Del Estero');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (90, 'Tucuman');
Insert into identificador_provincias(id_provincia_indec, nombre_provincia_indec) Values (94, 'Tierra Del Fuego');


-- Tabla de identificador_localidades
CREATE TABLE identificador_localidades (
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

CREATE TABLE dp_puestostrabajo_total (
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
CREATE TABLE ipc_valores(
Fecha date not null,
ID_Region int,
ID_Categoria int, 
ID_Division int,
ID_Subdivision int,
Valor float,

Foreign key (ID_Region) references identificador_regiones(ID_Region),
Foreign key (ID_Categoria) references ipc_categoria(id_categoria)
);

create table ipc_categoria(
id_categoria INT,
nombre VARCHAR(255) NOT NULL,

primary key (id_categoria)
);
INSERT INTO ipc_categoria (id_categoria, nombre) VALUES
    (1, 'Nivel general'),
    (2, 'Alimentos y bebidas no alcohólicas'),
    (3, 'Bebidas alcohólicas y tabaco'),
    (4, 'Prendas de vestir y calzado'),
    (5, 'Vivienda, agua, electricidad, gas y otros combustibles'),
    (6, 'Equipamiento y mantenimiento del hogar'),
    (7, 'Salud'),
    (8, 'Transporte'),
    (9, 'Comunicación'),
    (10, 'Recreación y cultura'),
    (11, 'Educación'),
    (12, 'Restaurantes y hoteles'),
    (13, 'Bienes y servicios varios');


create table ipc_division(
id_categoria int,
id_division int,
nombre varchar(150),

primary key (id_division),
foreign key (id_categoria) references ipc_categoria(id_categoria)
);
INSERT INTO ipc_division (id_categoria, id_division, nombre) VALUES
    (1, 1, 'Nivel general'),
    (2, 2, 'Alimentos y bebidas no alcohólicas'),
    (2, 3, 'Alimentos'),
    (2, 4, 'Bebidas no alcohólicas'),
    (3, 5, 'Bebidas alcohólicas y tabaco'),
    (3, 6, 'Bebidas alcohólicas'),
    (3, 7, 'Tabaco'),
    (4, 8, 'Prendas de vestir y calzado'),
    (4, 9, 'Prendas de vestir y materiales'),
    (4, 10, 'Calzado'),
    (5, 11, 'Vivienda, agua, electricidad, gas y otros combustibles'),
    (5, 12, 'Alquiler de la vivienda y gastos conexos'),
    (5, 13, 'Mantenimiento y reparación de la vivienda'),
    (5, 14, 'Electricidad, gas y otros combustibles'),
    (6, 15, 'Equipamiento y mantenimiento del hogar'),
    (6, 16, 'Bienes y servicios para la conservación del hogar'),
    (7, 17, 'Salud'),
    (7, 18, 'Productos medicinales, artefactos y equipos para la salud'),
    (7, 19, 'Gastos de prepagas'),
    (8, 20, 'Transporte'),
    (8, 21, 'Adquisición de vehículos'),
    (8, 22, 'Funcionamiento de equipos de transporte personal'),
    (8, 23, 'Transporte público'),
    (9, 24, 'Comunicación'),
    (9, 25, 'Servicios  de telefonía e internet'),
    (10, 26, 'Recreación y cultura'),
    (10, 27, 'Equipos audiovisuales, fotográficos y de procesamiento de la información'),
    (10, 28, 'Servicios recreativos y culturales'),
    (10, 29, 'Periódicos, diarios, revistas, libros y artículos de papelería'),
    (11, 30, 'Educación'),
    (12, 31, 'Restaurantes y hoteles'),
    (12, 32, 'Restaurantes y comidas fuera del hogar'),
    (13, 33, 'Bienes y servicios varios'),
    (13, 34, 'Cuidado personal');
    
create table ipc_subdivision(
id_categoria int,
id_division int,
id_subdivision int,
nombre varchar(150),

primary key (id_subdivision),
foreign key (id_division) references ipc_division(id_division)
);
INSERT INTO ipc_subdivision (id_categoria, id_division, id_subdivision, nombre) VALUES
    (1, 1, 1, 'Nivel general'),
    (2, 2, 2, 'Alimentos y bebidas no alcohólicas'),
    (2, 3, 3, 'Alimentos'),
    (2, 3, 4, 'Pan y cereales'),
    (2, 3, 5, 'Carnes y derivados'),
    (2, 3, 6, 'Leche, productos lácteos y huevos'),
    (2, 3, 7, 'Aceites, grasas y manteca'),
    (2, 3, 8, 'Frutas'),
    (2, 3, 9, 'Verduras, tubérculos y legumbres'),
    (2, 3, 10, 'Azúcar, dulces, chocolate, golosinas, etc,'),
    (2, 4, 11, 'Bebidas no alcohólicas'),
	(2, 4, 12, 'Café, té, yerba y cacao'),
	(2, 4, 13, 'Aguas minerales, bebidas gaseosas y jugos'),
    (3, 5, 14, 'Bebidas alcohólicas y tabaco'),
    (3, 6, 15, 'Bebidas alcohólicas'),
    (3, 7, 16, 'Tabaco'),
    (4, 8, 17, 'Prendas de vestir y calzado'),
    (4, 9, 18, 'Prendas de vestir y materiales'),
    (4, 10, 19, 'Calzado'),
    (5, 11, 20, 'Vivienda, agua, electricidad, gas y otros combustibles'),
    (5, 12, 21, 'Alquiler de la vivienda y gastos conexos'),
    (5, 12, 22, 'Alquiler de la vivienda'),
    (5, 13, 23, 'Mantenimiento y reparación de la vivienda'),
    (5, 14, 24, 'Electricidad, gas y otros combustibles'),
    (6, 15, 25, 'Equipamiento y mantenimiento del hogar'),
    (6, 16, 26, 'Bienes y servicios para la conservación del hogar'),
    (7, 17, 27, 'Salud'),
    (7, 18, 28, 'Productos medicinales, artefactos y equipos para la salud'),
    (7, 19, 29, 'Gastos de prepagas'),
    (8, 20, 30, 'Transporte'),
    (8, 21, 31, 'Adquisición de vehículos'),
    (8, 22, 32, 'Funcionamiento de equipos de transporte personal'),
    (8, 22, 33, 'Combustibles y lubricantes para vehículos de uso del hogar'),
    (8, 23, 34, 'Transporte público'),
    (9, 24, 35, 'Comunicación'),
    (9, 25, 36, 'Servicios  de telefonía e internet'),
    (10, 26, 37, 'Recreación y cultura'),
    (10, 27, 38, 'Equipos audiovisuales, fotográficos y de procesamiento de la información'),
    (10, 28, 39, 'Servicios recreativos y culturales'),
    (10, 29, 40, 'Periódicos, diarios, revistas, libros y artículos de papelería'),
    (11, 30, 41, 'Educación'),
    (12, 31, 42, 'Restaurantes y hoteles'),
    (12, 32, 43, 'Restaurantes y comidas fuera del hogar'),
    (13, 33, 44, 'Bienes y servicios varios'),
    (13, 34, 45, 'Cuidado personal');

create table ipc_productos(
ID_Region int,
Fecha date,
Producto char(50),
Valor float
);
    
-- TABLAS SIPA
CREATE TABLE sipa_registro(
Fecha date not null,
ID_Provincia int,
ID_Tipo_Registro int,
Cantidad_con_Estacionalidad float,
Cantidad_sin_Estacionalidad float,

Foreign key (ID_Provincia) references identificador_provincias(id_provincia_indec),
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

Foreign key (ID_Provincia) references identificador_provincias(id_provincia_indec)
);

Create Table dnrpa_inscripcion(
Fecha date not null,
ID_Vehiculo int,
ID_Provincia int, 
Region varchar(125),
Cantidad int,

Foreign key (ID_Provincia) references identificador_provincias(id_provincia_indec),
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

create table emae(
Fecha date not null,
Sector_Productivo char(125),
Valor float
);

-- TABLAS RIPTE ipecd_economico
create table ripte(
Fecha date,
ripte float
);
-- TABLAS RIPTE datalake_economico
create table ripte(
fecha date,
valor float
);

#Tabla De Salario Minimo Vital y Movil
CREATE TABLE salario_mvm(
	fecha date, 
    salario_mvm_mensual float,
    salario_mvm_diario float,
    salario_mvm_hora float
);

CREATE TABLE Canasta_Basica(
	Fecha date, 
	CBA_Adulto float,
	CBT_Adulto float,
	CBA_Hogar float,
	CBT_Hogar float
);

Create Table IPICORR(
	Fecha date,
    Var_Interanual_IPICORR float,
    Var_Interanual_Alimentos float,
    Var_Interanual_Textil float,
    Var_Interanual_Maderas float,
    Var_Interanual_MinNoMetalicos float,
    Var_Interanual_Metales float
);

#Dolar
Create Table dolar_oficial(
	fecha date,
    compra float,
    venta float
);
Create Table dolar_blue(
	fecha date,
    compra float,
    venta float
);
Create Table dolar_ccl(
	fecha date,
    valor float
);
Create Table dolar_mep(
	fecha date,
    valor float
    );

#Supermercado
Create Table supermercado_encuesta(
	id_provincia_indec INT,
    fecha date,
    bebidas double,
    almacen double,
    panaderia double,
    lacteos double,
    carnes double,
    verduleria_fruteria double, 
    alimentos_preparados_rostiseria double, 
    articulos_limpieza_perfumeria double,
    indumentaria_calzado_textiles_hogar double,
    electronica_hogar double,
    otros double
);

#ECV DWH Sociodemografico
Create table ecv_tasas(
	fecha date,
    trimestre int,
    aglomerado char(20),
    tasa_de_empleo float,
    tasa_de_desocupacion float,
    tasa_de_actividad float,
    tasa_de_inactividad float
);

Create table ecv_trabajo(
	aglomerado VARCHAR(255),
    año INT,
    fecha DATE,
    trimestre VARCHAR(255),
    tasa_de_actividad FLOAT,
    tasa_de_empleo FLOAT,
    tasa_de_desocupación FLOAT,
    trabajo_privado FLOAT,
    trabajo_público FLOAT,
    trabajo_otro FLOAT,
    trabajo_privado_registrado FLOAT,
    trabajo_privado_no_registrado FLOAT,
    salario_promedio_público FLOAT,
    salario_promedio_privado FLOAT,
    salario_promedio_privado_registrado FLOAT,
    salario_promedio_privado_no_registrado FLOAT,
    patron FLOAT,
    cuenta_propia FLOAT,
    empleado_obrero FLOAT,
    trabajador_familiar_sin_remuneración FLOAT
);

Create table ecv_trabajo_quintiles(
    aglomerado VARCHAR(255),
    año INT,
    fecha DATE,
    trimestre VARCHAR(255),
    quintil INT,
    empleo_público FLOAT,
    empleo_privado FLOAT,
    empleo_otro FLOAT,
    patron FLOAT,
    cuenta_propia FLOAT,
    obrero_o_empleado FLOAT,
    trabajador_familiar_sin_remuneracion FLOAT,
    primaria_incompleta FLOAT,
    primaria_completa FLOAT,
    secundaria_incompleta FLOAT,
    secundaria_completa FLOAT,
    superior_o_universitario_incompleto FLOAT,
    superior_o_universitario_completo FLOAT,
    sin_instruccion FLOAT
);

create table ecv_salud_cobertura(
	aglomerado char(100),
    año int,
	fecha DATE,
    semestre VARCHAR(255),
    cobertura float,
    planes_y_seguros float,
    no_paga_ni_le_descuentan float
);

create table ecv_salud_consulta_establecimiento(
	aglomerado char(100),
    año int,
	fecha DATE,
    semestre VARCHAR(255),
    cobertura VARCHAR(255),
    si_consulto float,
    no_consulto float,
    dolencia_afeccion_enfermedad float,
    control_prevencion float,
    establecimiento_privado float,
    establecimiento_publico float
);

create table ecv_salud_quintil_consulta(
	aglomerado char(100),
    año int,
	fecha DATE,
    semestre VARCHAR(255),
    cobertura VARCHAR(255),
	quintil int,
    si_consulto float,
    no_consulto float
);

create table ecv_salud_quintil_cobertura_est(
	aglomerado char(100),
    año int,
	fecha DATE,
    semestre VARCHAR(255),
    quintil int,
    obra_social_prepaga float,
    planes_y_seguros float,
    sin_cobertura float,
    establecimiento_privado float,
    establecimiento_publico float
);
create table ecv_educacion(
	aglomerado VARCHAR(255),
    año INT,
    trimestre VARCHAR(255),
    fecha DATE,
    nivel_educativo VARCHAR(255),
    asiste FLOAT,
    no_asiste_pero_asistio FLOAT,
    nunca_asistio FLOAT,
    institucion_publica FLOAT,
    institucion_privada FLOAT,
    edad_promedio_abandono FLOAT,
    sobreedad FLOAT,
    acceso_a_internet_fijo FLOAT,
    calidad_de_vivienda_suficiente FLOAT,
    calidad_de_vivienda_parcialmente_insuficiente FLOAT,
    calidad_de_vivienda_insuficiente FLOAT,
    vivienda_cercana_a_un_basural FLOAT,
    vivienda_en_villa_emergencia FLOAT,
    automovil FLOAT,
    motocicleta FLOAT,
    bicicleta FLOAT,
    caminata FLOAT,
    taxi_remis FLOAT,
    transporte_urbano FLOAT,
    otros FLOAT
);
create table ecv_educacion_may25(
	aglomerado VARCHAR(255),
    año INT,
    trimestre VARCHAR(255),
    fecha DATE,
	primaria_incompleta FLOAT,
    primaria_completa FLOAT,
    secundaria_incompleta FLOAT,
    secundaria_completa FLOAT,
    superior_incompleto FLOAT,
    superior_completo FLOAT,
    sin_instruccion FLOAT,
    eph_universitario FLOAT
);

create table ecv_educacion_quintiles(
	aglomerado VARCHAR(255),
    año INT,
    fecha DATE,
    trimestre VARCHAR(255),
    quintil INT,
    primaria_incompleta FLOAT,
    primaria_completa FLOAT,
    secundaria_incompleta FLOAT,
    secundaria_completa FLOAT,
    superior_incompleto FLOAT,
    superior_completo FLOAT,
    sin_instruccion FLOAT,
    asistencia_escolar FLOAT,
    institucion_publica FLOAT,
    institucion_privada FLOAT
);

#EPH Tablas
Create table eph_tasas(
	aglomerado char(100),
    año int,
    fecha date,
    trimestre char(100),
    tasa_de_actividad float,
    tasa_de_empleo float,
    tasa_de_desocupacion float
);
#PBG tablas
create table pbg_presupuesto_ejecutado(
	mes int,
	año int,
    jurisdiccion int,
    gastos_en_personal float,
    bienes_de_consumo float,
    servicios_no_personales float,
    bienes_de_uso float
);

#Tabla de Cantidad de Puestos Empleados cada Mil Habitantes
create table empleados_cada_mil_habitantes_provincia(
	fecha date,
    id_provincia int,
    puestos_cada_mil_empleados float
);
create table empleados_cada_mil_habitantes_corrientes(
	fecha date,
    provincia int,
    departamento int,
    puestos_cada_mil_empleados float
);

create table censo_sheets_ipecd_prueba(
	id_departamento INT,
    departamento VARCHAR(255),
    poblacion_2010 float,
    poblacion_2022 float,
    variacion_relativa FLOAT,
    densidad_de_habitantes_por_KM2 FLOAT,
    poblacion_2022_mujer_excluye_situacion_de_calle float,
    poblacion_2022_varon_excluye_situacion_de_calle float,
    indice_de_feminidad FLOAT,
    _2022_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100 FLOAT,
    _2010_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100 FLOAT,
    _2022_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64 FLOAT,
    _2010_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64 FLOAT,
    tasa_de_empleo FLOAT,
    tasa_de_desocup FLOAT,
    tasa_de_actividad FLOAT,
    categoria_ocupacional_servicio_domestico float,
    categoria_ocupacional_empleado_u_obrero float,
    categoria_ocupacional_cuenta_propia float,
    categoria_ocupacional_patron_o_empleador float,
    categoria_ocupacional_trabajador_familiar float,
    categoria_ocupacional_ignorado float,
    población_que_asiste_a_institución_educativa float,
    población_que_no_asiste_pero_asistio_a_institución_educativa float,
    población_que_nunca_asistio_a_institución_educativa float,
    pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03 float,
    pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5 float,
    pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario float,
    pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario float,
    pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers float,
    pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado float,
    pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado float,
    mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo float,
    promedio_de_hijos_por_mujer FLOAT,
    población_en_vivienda_obra_social_o_prepaga_incluye_PAMI float,
    población_en_vivienda_programas_o_planes_estatales_de_salud float,
    población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal float
)

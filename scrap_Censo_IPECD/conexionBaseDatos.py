import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from datetime import datetime
import calendar
import os
import xlrd
from sqlalchemy import create_engine

class conexcionBaseDatos:

    def __init__(self, id_departamento, departamento, poblacion_2010, poblacion_2022, variacion_relativa, densidad_habitantes_por_KM2, poblacion_2022_mujer_excluye_situación_de_calle, poblacion_2022_varon_excluye_situación_de_calle, indice_feminidad, _2022_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100, _2010_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100 , _2022_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64, _2010_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64, tasa_de_empleo, tasa_de_desocup, tasa_de_actividad, categoria_ocupacional_servicio_domestico, categoria_ocupacional_empleado_u_obrero, categoria_ocupacional_cuenta_propia, categoria_ocupacional_patron_o_empleador, categoria_ocupacional_trabajador_familiar, categoria_ocupacional_ignorado, población_que_asiste_a_institución_educativa, población_que_no_asiste_pero_asistio_a_institución_educativa, población_que_nunca_asistio_a_institución_educativa, pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03, pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5 , pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario , pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario , pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers , pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado ,  pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado  ,mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo, promedio_de_hijos_por_mujer, población_en_vivienda_obra_social_o_prepaga_incluye_PAMI, población_en_vivienda_programas_o_planes_estatales_de_salud , población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal, host, user, password, database):
        self.id_departamento = id_departamento
        self.departamento = departamento
        self.poblacion_2010 = poblacion_2010
        self.poblacion_2022 = poblacion_2022
        self.variacion_relativa = variacion_relativa 
        self.densidad_habitantes_por_KM2  = densidad_habitantes_por_KM2 
        self.poblacion_2022_mujer_excluye_situación_de_calle = poblacion_2022_mujer_excluye_situación_de_calle
        self.poblacion_2022_varon_excluye_situación_de_calle = poblacion_2022_varon_excluye_situación_de_calle
        self.indice_feminidad = indice_feminidad
        self._2022_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100  = _2022_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100 
        self._2010_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100  = _2010_Índ_de_envej_mas_65_años_sob_per_0_a_14_años_por_100 
        self._2022_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64  = _2022_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64  
        self._2010_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64  = _2010_índ_de_dep_potenc_0_a_14_mas_65_o_más_sob_per_de_15_a_64 
        self.tasa_de_empleo = tasa_de_empleo
        self.tasa_de_desocup = tasa_de_desocup
        self.tasa_de_actividad = tasa_de_actividad
        self.categoria_ocupacional_servicio_domestico = categoria_ocupacional_servicio_domestico
        self.categoria_ocupacional_empleado_u_obrero = categoria_ocupacional_empleado_u_obrero
        self.categoria_ocupacional_cuenta_propia = categoria_ocupacional_cuenta_propia
        self.categoria_ocupacional_patron_o_empleador = categoria_ocupacional_patron_o_empleador
        self.categoria_ocupacional_trabajador_familiar = categoria_ocupacional_trabajador_familiar
        self.categoria_ocupacional_ignorado = categoria_ocupacional_ignorado
        self.población_que_asiste_a_institución_educativa = población_que_asiste_a_institución_educativa
        self.población_que_no_asiste_pero_asistio_a_institución_educativa = población_que_no_asiste_pero_asistio_a_institución_educativa
        self.población_que_nunca_asistio_a_institución_educativa = población_que_nunca_asistio_a_institución_educativa
        self.pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03  = pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03 
        self.pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5 = pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5  
        self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario  = pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario 
        self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario  = pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario 
        self.pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers  = pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers 
        self.pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado  = pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado 
        self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado  = pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado 
        self.mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo = mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo
        self.promedio_de_hijos_por_mujer = promedio_de_hijos_por_mujer
        self.población_en_vivienda_obra_social_o_prepaga_incluye_PAMI = población_en_vivienda_obra_social_o_prepaga_incluye_PAMI
        self.población_en_vivienda_programas_o_planes_estatales_de_salud = población_en_vivienda_programas_o_planes_estatales_de_salud
        self.población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal = población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal
       
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
    
    def cargaBaseDatos(self):
        print("\n*****************************************************************************")
        print("***********************Inicio de la seccion Censo IPECD************************")
        print("\n*****************************************************************************")

        self.conectar_bdd()
        df = pd.DataFrame()
        df['Id_Departamento'] = self.id_departamento
        df['Departamento'] = self.departamento
        df['Poblacion 2010'] = self.poblacion_2010
        df['Poblacion 2022'] = self.poblacion_2022
        df['Variacion relativa %'] = self.variacion_relativa
        df['Densidad de habitantes por KM2'] = self.densidad_habitantes_por_KM2
        df['Poblacion 2022 mujer (excluye situacion de calle)'] = self.poblacion_2022_mujer_excluye_situación_de_calle
        df['Poblacion 2022 varon (excluye situacion de calle)'] = self.poblacion_2022_varon_excluye_situación_de_calle
        df['Indice de feminidad'] = self.indice_feminidad
        df['Tasa de empleo'] = self.tasa_de_empleo
        df['Tasa de desocup'] = self.tasa_de_desocup
        df['Tasa de actividad'] = self.tasa_de_actividad
        df['Cateogría ocupacional: Servicio doméstico'] = self.categoria_ocupacional_servicio_domestico
        df['Categoría Ocupacional: Empleada(o) u obrera(o)'] = self.categoria_ocupacional_empleado_u_obrero
        df['Categoría Ocupacional: Cuenta propia'] = self.categoria_ocupacional_cuenta_propia 
        df['Categoría Ocupacional: Patrón(a) o empleador(a)'] = self.categoria_ocupacional_patron_o_empleador
        df['Categoría Ocupacional: Trabajador(a) familiar'] = self.categoria_ocupacional_trabajador_familiar
        df['Categoría Ocupacional: Ignorado'] = self.categoria_ocupacional_ignorado 
        df['Población que asiste a institución educativa'] = self.población_que_asiste_a_institución_educativa
        df['Población que no asiste pero asistió a inst educativa'] = self.población_que_no_asiste_pero_asistio_a_institución_educativa
        df['Población que nunca asistió a inst educativa'] = self.población_que_nunca_asistio_a_institución_educativa
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo Jardin maternal, guardería, centro de cuidado, salas de 0 a 3'] = self.pob_en_viv_part_q_asis_a_esc_niv_educ_mat_guard_cen_cuid_sal_03
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo sala de 4 o 5 años'] = self.pob_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_o_5
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo primario'] = self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_primario
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo secundario'] = self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_secundario
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo terciario no universitario'] = self.pob_en_viv_part_que_asis_a_esc_niv_educ_terciario_no_univers
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo universitario de grado'] = self.pob_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado
        df['Población en viviendas particulares que asiste a escuelas: nivel educativo posgrado'] = self.pob_en_viv_part_que_asiste_a_escuelas_nivel_educ_posgrado
        df['Mujeres de 14 a 49 años con al menos 1 hijo nacido vivo'] = self.mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo
        df['Promedio de hijos por mujer'] = self.promedio_de_hijos_por_mujer
        df['Población en vivienda: Obra Social o prepaga (incluye PAMI)'] = self.población_en_vivienda_obra_social_o_prepaga_incluye_PAMI
        df['Población en vivienda: Programas o planes estatales de salud'] = self.población_en_vivienda_programas_o_planes_estatales_de_salud
        df['Población en vivienda: No tiene obra social, prepaga ni plan estatal'] = self.población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal



        print(df)

        # Sentencia SQL para insertar los datos en la tabla
        insert_query = "INSERT INTO censo_sheets_ipecd (id_departamento, departamento, poblacion_2010, poblacion_2022, variacion_absoluta, variacion_relativa, superficie_KM2, densidad_habitantes, poblacion_2022_mujer_excluye_situación_de_calle, poblacion_2022_varon_excluye_situación_de_calle, indice_feminidad, pobl_en_viviendas_particulares, pobl_en_viv_part_con_techo_reves_interior_o_cielora, pobl_en_viviendas_particulares_con_pisos_revestimiento, pobl_en_viv_part_con_pisos_Carpeta_contrap_o_ladri_fijo, pobl_en_viv_part_con_pisos_Tierra_o_ladrillo_suelto, pobl_en_viviendas_particulares_con_pisos_Otro_material, pobl_en_viv_part_con_proced_del_agua_cañería_dentro_de_la_viv, pobl_en_viv_part_con_proced_del_agua_fuera_viv_dent_del_terr, pobl_en_viv_part_con_proced_del_agua_fuera_del_terreno, pobl_en_viv_part_con_prov_de_agua_por_red_pública_agua_cte, pobl_en_viv_part_con_desagüe_del_inodoro_a_red_públ_cloaca, desag_y_desc_de_agua_del_inod_A_red_públ_A_cám_sépt_pozo_ciego, desag_y_desc_de_agua_del_inod_Solo_a_pozo_ciego, desag_y_desc_de_agua_del_inod_A_hoyo_exc_en_tierra_etc, desag_y_desc_de_agua_del_inod_No_tiene, pob_en_viv_part_utiliza_gas_mas_electricidad_para_cocinar, pob_en_viv_part_utiliza_princip_para_cocinar_electricidad, pob_en_viv_part_utiliza_princip_para_cocinar_gas_de_red, pob_en_viv_part_uti_princip_para_coc_Gas_tubo_o_granel_zeppelin, pob_en_viv_part_utiliza_princip_para_cocinar_gas_en_garrafa, pob_en_viv_part_utiliza_princip_para_cocinar_leña_o_carbon, pob_en_viv_part_utiliza_princip_para_cocinar_otro_combustible, pob_en_viv_part_cuya_vivienda_es_propia, pob_en_viv_part_cuya_vivienda_es_alquilada, pob_en_viv_part_cuya_vivienda_es_cedida_por_trabajo, pob_en_viv_part_cuya_vivienda_es_prestada, pob_en_viv_part_cuya_vivienda_es_otra_situacion, tiene_internet_en_la_vivienda_Tiene_computadora_tablet_etc, tiene_internet_en_la_vivienda_No_Tiene_computadora_tablet_etc, no_tiene_internet_en_la_vivienda_Tiene_computadora_tablet_etc, no_tiene_internet_en_la_vivienda_No_Tiene_computadora_tablet_etc, tiene_internet_en_la_vivienda_Tiene_celular_con_internet, tiene_internet_en_la_vivienda_No_Tiene_celular_con_internet, no_tiene_internet_en_la_vivienda_Tiene_celular_con_internet, no_tiene_internet_en_la_vivienda_No_Tiene_celular_con_internet, población_en_vivienda_Percibe_solo_jubilación, población_en_vivienda_Percibe_solo_pension_por_fallecimiento, población_en_vivienda_Percibe_jubilación_y_pension_por_fallec, población_en_vivienda_Percibe_solo_pension_de_otro_tipo, población_en_vivienda_No_Percibe_ni_jubilación_ni_pension, población_en_vivienda_obra_social_o_prepaga_incluye_PAMI, población_en_vivienda_programas_o_planes_estatales_de_salud, población_en_viv_no_tiene_obra_social_prepaga_ni_plan_estatal, población_en_viv_particulares_poblacion_que_asiste_a_escuelas, población_en_viv_part_que_no_asiste_pero_asistio_a_escuelas, población_en_viv_part_que_nunca_asistio_a_escuelas, pob_en_viv_part_q_asis_niv_jard_mat_guard_cen_cuid_sal_03, pobl_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_a_5_años, pobl_en_viv_part_que_asiste_a_esc_niv_educ_primario, pobl_en_viv_part_que_asiste_a_esc_niv_educ_secundario, pobl_en_viv_part_que_asis_a_esc_niv_educ_terci_no_univ, pobl_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado, pobl_en_viv_part_que_asiste_a_esc_niv_educ_posgrado, población_en_viviendas_colectivas, edad_mediana_población, edad_mediana_mujer, edad_mediana_varon, _2022_Índ_de_envej_mas_65_años_o_pers_0_a_14_años_por_100, _2010_Índ_de_envej_mas_65_años_o_pers_0_a_14_años_por_100, _2022_índ_de_depen_potenc_0_a_14_y_65_o_más_o_pers_de_15_a_64, _2010_índ_de_depen_potenc_0_a_14_y_65_o_más_o_pers_de_15_a_64, población_de_80_años_y_más, total_de_viviendas_2022, viviendas_particulares_2022, viviendas_particulares_2010, variación_viv_part_abs_2022_2010, variación_viv_part_rel_2022_2010, viviendas_particulares_hay_personas_presentes, no_pers_pres_viv_se_usa_para_vac_fin_semana_seg_resid_uso_temp, no_hay_pers_pres_viv_se_usa_como_oficina_consultorio_o_comercio, no_hay_pers_pres_La_vivienda_esta_en_alquiler_o_venta, no_hay_pers_pres_La_vivienda_esta_en_construccion, no_hay_pers_pres_Habit_viven_pero_no_se_encuentran_presentes, no_hay_personas_presentes_Otra_situacion, viviendas_colectivas, viv_part_con_personas_presentes_Tipo_de_vivienda_Casa, viv_part_con_personas_presentes_Tipo_de_vivienda_Rancho, viv_part_con_personas_presentes_Tipo_de_vivienda_Casilla, viv_part_con_personas_presentes_Tipo_de_vivienda_Depart, viv_part_con_pers_pres_Tip_de_viv_Piez_inquil_hotel_famil_o_pens, viv_part_con_pers_pres_Tip_de_viv_Loc_no_constr_para_habit_ocup, viv_part_con_pers_pres_Tip_de_viv_móvil_ocupad_cas_rod_barc_carp, total_de_hogares, hog_en_Viv_part_con_pisos_con_revestimiento, hog_en_Viv_part_con_pisos_con_carpeta_contrap_o_ladrillo_fijo, hog_en_Viv_part_con_pisos_con_tierra_o_ladrillo_suelto, hog_en_Viv_part_con_pisos_con_otro_material, hog_en_Viv_part_con_proced_agua_por_cañeria_dentro_de_la_viv, hog_en_Viv_part_con_proced_agua_fuera_de_viv_dentro_del_terr, hog_en_Viv_part_con_proced_agua_fuera_del_terreno, region, tasa_de_empleo, tasa_de_desocup, tasa_de_actividad, categoria_ocupacional_servicio_domestico, categoria_ocupacional_empleado_u_obrero, categoria_ocupacional_cuenta_propia, categoria_ocupacional_patron_o_empleador, categoria_ocupacional_trabajador_familiar, categoria_ocupacional_ignorado, población_que_asiste_a_institución_educativa, población_que_no_asiste_pero_asistio_a_institución_educativa, población_que_nunca_asistio_a_institución_educativa, pob_en_viv_part_que_asis_niv_jard_mat_guard_cen_cuid_sal_03, pobl_en_viv_part_que_asis_a_esc_niv_educ_sala_de_4_a_5_años, pobl_en_viv_part_que_asiste_a_esc_niv_educ_primario, pobl_en_viv_part_que_asiste_a_esc_niv_educ_secundario, pobl_en_viv_part_que_asis_a_esc_niv_educ_terci_no_univ, pobl_en_viv_part_que_asiste_a_esc_niv_educ_univ_de_grado, pobl_en_viv_part_que_asiste_a_esc_niv_educ_posgrado, mujeres_de_14_a_49_años_en_viviendas_particulares, mujeres_de_14_a_49_años_con_al_menos_1_hijo_nacido_vivo, promedio_de_hijos_por_mujer) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="censo_sheets_ipecd", con=engine, if_exists='replace', index=False)

        
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()


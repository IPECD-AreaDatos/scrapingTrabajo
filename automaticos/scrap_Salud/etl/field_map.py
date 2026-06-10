"""
Diccionario de mapeo de campos por fuente hacia la tabla destino pacientes_gold.

Generado automáticamente desde:
WP_DatosSolicitar_ListadoGeneralEmbarazos.xlsx → Hoja "Mapeo"

Estructura del Excel:
- Col 2: Campo fuente POF (1ra prioridad)
- Col 4: Campo fuente WP (2da prioridad)
- Col 6: Campo fuente Derivaciones (3ra prioridad)
- Col 8: Campo fuente SUMAR (4ta prioridad)
- Col 10: Campo DESTINO en pacientes_gold

Prioridad de fuentes para mismos DNI:
1. POF (col 2) - Máxima prioridad
2. WP (col 4)
3. SUMAR (col 8)
4. Derivaciones (col 6) - Mínima prioridad

Última actualización: 2026-04-21
"""

# ════════════════════════════════════════════════════════════════════════════
# MAPEO DE CAMPOS POR FUENTE (generado desde Excel)
# ════════════════════════════════════════════════════════════════════════════

FIELD_MAP = {
    # Fuente: Vista SQL Server (reemplaza POF/WP) - 1ra prioridad
    "v_embarazosdw": {
        "DNI": "dni",
        "Apellido": "apellido",
        "Nombre": "nombre",
        "FechaNacimiento": "fecha_nacimiento",
        "Cob. Salud": "cobertura_salud",
        "EmbarazoEstado": "embarazo_en_curso",
        "EsPuerpera": "puerpera",
        "CentroSalud": "nombre_establecimiento",
        "FechaProbableParto": "fecha_probable_parto",
        "FUM": "fum",
        "EmbarazoRiesgo": "riesgo",
        "Ctrls 1° T": "controles_1er_trim",
        "Ctrls 2° T": "controles_2do_trim",
        "Ctrls 3° T": "controles_3er_trim",
        "Ult Ctrl": "fecha_ultimo_control",
        "Deriv Centro": "nombre_centro_derivado",
        "FechaDerivacion": "fecha_derivacion",
        "MotivoDerivacion": "motivo_diagnostico_derivacion",
        "Telefono": "telefono",
        "Domicilio Depto": "departamento_domicilio",
        "Domicilio Barrio": "barrio_paraje_domicilio",
        "Domicilio Calle": "calle_domicilio",
        "Domicilio Nro": "nro_puerta_domicilio",
        "LocalidadEmbarazada": "localidad_domicilio",
        "CodigoSISA": "sisa_centro_salud",
        "Deriv Centro SISA": "sisa_centro_derivado",
        "EmbarazoId": "embarazo_id",
        "EmbarazadaId": "embarazada_id",
        "LocalidadCentroSalud": "localidad_establecimiento",
        "FechaInicioSeguimiento": "fecha_inicio_seguimiento",
        "Controles": "controles",
        "Tratamientos": "tratamientos",
        "CantidadControles": "cantidad_controles",
        "CantidadTratamientos": "cantidad_tratamientos",
        "Fin Tipo": "fin_tipo",
        "Fin Fecha Gestacion": "fin_fecha_gestacion",
        "Reg F.H": "fecha_registro",
    },

    # Fuente: POF (Programa Obstétrico Femenino) - 1ra prioridad
    "pof": {
        "Nobre Establecimiento":    "nombre_establecimiento",
        "DNI Embarazada":           "dni",
        "Ay N":                     "apellido",
        "Fecha Nac":                "fecha_nacimiento",
        "Edad Actual":              "edad_actual",
        "Cob. Salud":               "cobertura_salud",
        "Diagnostico Embarazo":     "fecha_diagnostico_embarazo",
        "FUM":                      "fum",
        "Fecha PP":                 "fecha_probable_parto",
        "Riesgo":                   "riesgo",
        "EG Calculada":             "eg_actual",
        "CantControles":            "cantidad_controles",
        "Ult. Control":             "fecha_ultimo_control",
        "Tipo Doc.":                "tipo_documento",
        "Telefono":                 "telefono",
        "Domicilio Depto":          "departamento_domicilio",
        "Domicilio Barrio":         "barrio_paraje_domicilio",
        "Domicilio Calle":          "calle_domicilio",
        "Domicilio Nro":            "nro_puerta_domicilio",
        "Localidad":                "localidad_domicilio",
    },

    # Fuente: WP (Listado General) - 2da prioridad
    "wp": {
        "Localidad":                "localidad",
        "Centro Salud":             "nombre_centro_salud",
        "En Curso":                 "embarazo_en_curso",
        "Puerpera":                 "puerpera",
        "Embarazada DNI":           "dni",
        "Embarazada Ay N":          "apellido",
        "Embarazada Fecha Nac":     "fecha_nacimiento",
        "Edad Actual":              "edad_actual",
        "Cob. Salud":               "cobertura_salud",
        "Inicio Fecha":             "fecha_diagnostico_embarazo",
        "FUM":                      "fum",
        "Edad Al Inicio":           "edad_al_inicio",
        "Fecha PP":                 "fecha_probable_parto",
        "Riesgo":                   "riesgo",
        "Nro. Embarazo":            "numero_embarazo",
        "Edad Gestacional":         "eg_actual",
        "Ctrls 1° T":               "controles_1er_trim",
        "Ctrls 2° T":               "controles_2do_trim",
        "Ctrls 3° T":               "controles_3er_trim",
        "Cant Controles":           "cantidad_controles",
        "Ult Ctrl":                 "fecha_ultimo_control",
        "Controles":                "controles_obs",
        "Tratamientos":             "tratamiento",
        "Fin Tipo":                 "fin_tipo",
        "Fin Fecha Gestacion":      "fin_fecha_gestacion",
        "Deriv. Centro":            "nombre_centro_derivado",
        "Deriv. Fecha":             "fecha_derivacion",
    },

    # Fuente: Derivaciones (Red Obstétrica) - 4ta prioridad (mínima)
    "derivaciones": {
        "EFECTOR que deriva":       "nombre_establecimiento",
        "MATERNIDAD que recibe":    "nombre_centro_derivado",
        "DNI":                      "dni",
        "APELLIDO Y NOMBRE":        "apellido",
        "FECHA NACIMIENTO":         "fecha_nacimiento",
        "edad embarazada":          "edad_al_inicio",
        "PRIMER  CONTROL":          "observaciones_riesgo",
        "Nro de gestas":            "numero_embarazo",
        "edad gestacional":         "eg_actual",
        "Fecha último control":     "fecha_ultimo_control",
        "FECHA":                    "fecha_derivacion",
        "DIAGNOSTICO DE DERIVACION MATERNA": "motivo_diagnostico_derivacion",
        "Codigo SISA":              "sisa_centro_salud",
        "Codigo SISA Derivado":     "sisa_centro_derivado",
        "CANTIDAD CONTROLES":       "cantidad_controles",
        "MEDICO DERIVADOR":         "medico_deriva",
        "MEDICO RECEPTOR":          "medico_recibe",
        "maternidad_id":            "derivacion_maternidad_id",
    },

    # Fuente: Embarazadas Derivadas Alto Riesgo CAPS - prioridad equivalente a derivaciones
    "alto_riesgo_caps": {
        "DNI":                                                  "dni",
        "Nombre y Apellido":                                    "apellido",  # split posterior
        "F. de Nac.":                                           "fecha_nacimiento",
        "CAPS":                                                 "nombre_establecimiento",  # se normaliza por alias
        "Sem Gest":                                             "eg_actual",
        "Fecha de Derivacion":                                  "fecha_derivacion",
        "Motivo de derivacion":                                 "motivo_diagnostico_derivacion",
        "Nombre Hoja Sheet (HOSPITAL VIDAL / HOSPITAL LLANO)":  "nombre_centro_derivado",
        "maternidad_id":                                         "derivacion_maternidad_id",
    },

    # Fuente: SUMAR - 3ra prioridad
    "sumar": {
        "Nombre":                   "nombre_establecimiento",
        "afiDNI":                   "dni",
        "afiApellido":              "apellido",
        "afiNombre":                "nombre",
        "afiFechaNac":              "fecha_nacimiento",
        "Edad":                     "edad_actual",
        "afiTipoDoc":               "tipo_documento",
        "afiTelefono":              "telefono",
        "afiDomDepartamento":       "departamento_domicilio",
        "afiDomBarrioParaje":       "barrio_paraje_domicilio",
        "afiDomCalle":              "calle_domicilio",
        "afiDomNro":                "nro_puerta_domicilio",
        "afiDomLocalidad":          "localidad_domicilio",
        "afiDomPisoDpto":           "piso_dpto_domicilio",
        "afiNumero":                "afiliado_numero",
        "afiTipo":                  "afiliado_tipo",
        "beneficiarioTipo":         "beneficiario_tipo",
        "Programa":                 "programa",
        "NumInscripcion":           "numero_inscripcion",
        "FechaAltaAf":              "fecha_alta_af",
        "NombreEfectoraAf":         "nombre_efectora_af",
        "LocalidadAf":              "localidad_afiliacion",
        "CUIEAf":                   "cuie_afiliacion",
        "FechaDiagnosticoEmbarazo": "fecha_diagnostico_embarazo",
        "FUM":                      "fum",
        "FechaProbableParto":       "fecha_probable_parto",
        "SemanasEmbarazo":          "eg_actual",
        "EmbarazoActual":           "embarazo_en_curso",
        "PrenatalAltaFecha":        "prenatal_alta_fecha",
        "PrenatalAltaEfector":      "prenatal_alta_efector",
        "PrenatalAltaCUIE":         "prenatal_alta_cuie",
        "PrenatalControlFecha":     "prenatal_control_fecha",
        "PrenatalControlEfector":   "prenatal_control_efector",
        "estadoAfiliado":           "estado_afiliado",
        "TramoEmbarazo":            "tramo_embarazo",
        "EGSegunDiag":              "eg_segun_diagnostico",
        "FechaInicioSegunDiag":     "fecha_inicio_segun_diagnostico",
        "AR_Segun_Fact_Prest":      "ar_segun_fact_prestacion",
        "Diag_Desc":                "diagnostico_ar_sumar",
        "Fecha_Prest":              "fecha_prestacion_sumar",
        "Efector_Cuie":             "cuie_efector_prestacion",
        "Diag_Cod":                 "codigo_diagnostico",
        "Efector_Prest":            "nombre_efector_prestacion",
        "Localidad_Prest":          "localidad_prestacion_sumar",
        "ClaveBeneficiario":        "clave_beneficiario",
        "Activo":                   "activo",
        "afiDomManzana":            "manzana_domicilio",
        "afiDomDepto":              "depto_domicilio",
        "CUIEEfectorAsignado":      "cuie_seguimiento",
        "CEB":                      "ceb",
        "FechaUltimaPrestacion":    "fecha_ultima_prestacion",
        "CodigoPrestacion":         "codigo_prestacion",
        "DependenciaAdm":           "dependencia_administrativa",
        "DependenciaSanit":         "dependencia_sanitaria",
        "CodigoProvincialEfector":  "codigo_provincial_efector",
    },
}

# ════════════════════════════════════════════════════════════════════════════
# PRIORIDAD DE FUENTES (para consolidación de mismos DNI)
# ════════════════════════════════════════════════════════════════════════════
# Orden descendente: 1ra (más prioridad) → 4ta (menos prioridad)
FUENTE_PRIORIDADES = {
    "v_embarazosdw": 1,  # Nueva prioridad máxima
    "pof": 2,
    "wp": 3,
    "sumar": 4,
    "derivaciones": 5,
    "alto_riesgo_caps": 5,
}

# ════════════════════════════════════════════════════════════════════════════
# CLAVE NATURAL DEL PACIENTE
# ════════════════════════════════════════════════════════════════════════════
NATURAL_KEY = ("dni", "fecha_probable_parto")

# ════════════════════════════════════════════════════════════════════════════
# TIPO DE CÓDIGO DE ESTABLECIMIENTO POR FUENTE
# ════════════════════════════════════════════════════════════════════════════
ESTABLISHMENT_CODE_TYPE = {
    "pof":          "sisa",
    "wp":           "sisa",
    "derivaciones": "sisa",
    "sumar":        "cuie",
}

# ════════════════════════════════════════════════════════════════════════════
# CAMPOS COMUNES ENTRE FUENTES (para matching)
# ════════════════════════════════════════════════════════════════════════════
COMMON_FIELDS = [
    "dni",
    "apellido",
    "nombre",
    "fecha_nacimiento",
    "edad_actual",
    "fecha_probable_parto",
    "riesgo",
    "fecha_ultimo_control",
    "telefono",
    "cobertura_salud",
    "tipo_documento",
]

# ════════════════════════════════════════════════════════════════════════════
# CAMPOS EXCLUSIVOS POR FUENTE (se importan tal cual)
# ════════════════════════════════════════════════════════════════════════════
EXCLUSIVE_FIELDS = {
    "pof": [
        "nombre_establecimiento", "departamento_domicilio",
        "barrio_paraje_domicilio", "calle_domicilio",
        "nro_puerta_domicilio", "localidad_domicilio",
    ],
    "wp": [
        "localidad", "nombre_centro_salud", "embarazo_en_curso", "puerpera",
        "edad_al_inicio", "numero_embarazo", "controles_1er_trim",
        "controles_2do_trim", "controles_3er_trim", "controles_obs",
        "tratamiento", "fin_tipo", "fin_fecha_gestacion",
        "nombre_centro_derivado", "fecha_derivacion",
    ],
    "derivaciones": [
        "motivo_diagnostico_derivacion", "region_sanitaria",
        "medico_derivador", "medico_receptor",
    ],
    "sumar": [
        "afiliado_numero", "afiliado_tipo", "beneficiario_tipo",
        "programa", "numero_inscripcion", "fecha_alta_af",
        "nombre_efectora_af", "localidad_afiliacion", "cuie_afiliacion",
        "estado_afiliado", "tramo_embarazo", "eg_segun_diagnostico",
        "fecha_inicio_segun_diagnostico", "prenatal_alta_fecha",
        "prenatal_alta_efector", "prenatal_alta_cuie",
        "prenatal_control_fecha", "prenatal_control_efector",
        "ar_segun_fact_prestacion", "diagnostico_ar_sumar",
        "fecha_prestacion_sumar", "cuie_efector_prestacion",
        "codigo_diagnostico", "nombre_efector_prestacion",
        "localidad_prestacion_sumar", "clave_beneficiario", "activo",
        "manzana_domicilio", "depto_domicilio", "piso_dpto_domicilio",
        "cuie_seguimiento", "ceb", "fecha_ultima_prestacion",
        "codigo_prestacion", "dependencia_administrativa",
        "dependencia_sanitaria", "codigo_provincial_efector",
    ],
}

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE ARCHIVOS FUENTE
# ════════════════════════════════════════════════════════════════════════════
SOURCE_FILES = {
    "pof": {
        "tipo": "google_sheets",
        "archivo": "20260410_EmbarazosEnCurso_POF.xlsx",
        "hoja": "embarazadas",
    },
    "wp": {
        "tipo": "google_sheets",
        "archivo": "WPListadoGeneralEmbarazos.xlsx",
        "hoja": "embarazadas",
    },
    "derivaciones": {
        "tipo": "google_sheets",
        "archivo": "DERIVACIONES 2026 RED obstetrica AR",
        "hoja": "ENERO/FEBRERO/MARZO/ABRIL",
    },
    "sumar": {
        "tipo": "google_sheets",
        "archivo": "EmbarazadasSUMAR.xlsx",
        "hoja": "Embarazadas",
    },
}

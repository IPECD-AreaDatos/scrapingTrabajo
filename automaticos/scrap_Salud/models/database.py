from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey, Text, Boolean, Float, SmallInteger, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from flask_login import UserMixin
import datetime

Base = declarative_base()

# ─────────────────────────────────────────────────────────────────────────────
# Tablas de soporte / maestros
# ─────────────────────────────────────────────────────────────────────────────

class Usuario(Base, UserMixin):
    __tablename__ = 'usuarios'

    id            = Column(Integer, primary_key=True, autoincrement=True)
    username      = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role          = Column(String(50), nullable=False)

    # RBAC: Efector/Establecimiento se vincula por código SISA, Maternidad por ID interno.
    # sisa_code: código SISA del establecimiento (rol efector) — filtra por sisa_centro_salud en pacientes_gold.
    # maternidad_id: FK a maternidades (rol maternidad) — filtra por sisa_centro_salud + derivacion_maternidad_id.
    sisa_code      = Column(String(50), nullable=True, index=True)
    maternidad_id  = Column(Integer, ForeignKey('maternidades.id'), nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Efector(Base):
    """
    Tabla maestra de efectores/establecimientos de salud.
    Contiene el código SISA (oficial nacional) y el CUIE (Programa SUMAR).
    El campo 'cuie' es el usado como FK en el sistema.
    """
    __tablename__ = 'efectores_sisa'

    id          = Column(Integer, primary_key=True, autoincrement=True)
    cuie                     = Column(String(50), unique=True, index=True)   # Código SUMAR
    codigo_sisa              = Column(String(50), nullable=True, index=True) # Código SISA oficial
    nombre                   = Column(String(255), nullable=False)
    dependencia              = Column(String(100))
    codigo_indec_provincia   = Column(String(20))
    codigo_indec_departamento= Column(String(20))
    provincia                = Column(String(100))
    departamento             = Column(String(100))
    codigo_indec_localidad   = Column(String(20))
    localidad                = Column(String(100))
    ciudad                   = Column(String(100))
    domicilio                = Column(String(255))
    latitud                  = Column(String(50))
    longitud                 = Column(String(50))


class Maternidad(Base):
    __tablename__ = 'maternidades'

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    # Estructura alineada con data/Maternidades.xlsx
    codigo_sisa        = Column(String(50), unique=True, index=True)
    cuie               = Column(String(50), unique=True, index=True)
    nombre             = Column(String(255), nullable=False)   # Columna "Maternidad"
    categoria          = Column(String(50))                    # Columna "Categoria"

    # Compatibilidad con código previo
    localidad          = Column(String(100))
    nivel_complejidad  = Column(String(50))


# ─────────────────────────────────────────────────────────────────────────────
# Tabla principal de pacientes (Capa Gold)
# ─────────────────────────────────────────────────────────────────────────────

class PacienteGold(Base):
    """
    Tabla consolidada de embarazadas. Integra datos de 4 fuentes.

    Organización por capas:
    ├── CAPA CLÍNICA           Datos de la paciente y su embarazo (del mapeo de fuentes)
    │   ├── Grupo A            Identificación de la paciente
    │   ├── Grupo B            Datos del embarazo
    │   ├── Grupo C            Derivaciones
    │   ├── Grupo D            Contacto y domicilio
    │   └── Grupo E            Campos específicos SUMAR
    └── CAPA OPERATIVA         Trazabilidad y seguimiento (no proviene de fuentes externas)
        ├── Vínculos RBAC      FKs a efector y maternidad para filtrado por rol
        └── Auditoría ETL      Metadatos de ingesta (fuente, lote, fecha, observaciones)

    Clave natural: (dni, fecha_probable_parto)
    Permite diferenciar embarazos distintos de la misma paciente.
    """
    __tablename__ = 'pacientes_gold'

    # ── PK interna ──────────────────────────────────────────────────────────
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ════════════════════════════════════════════════════════════════════════
    # GRUPO A — Identificación de la Paciente
    # ════════════════════════════════════════════════════════════════════════
    dni                  = Column(String(20),  nullable=False, index=True)
    apellido             = Column(String(150))
    nombre               = Column(String(100))
    fecha_nacimiento     = Column(Date)
    edad_actual          = Column(Integer)
    tipo_documento       = Column(String(20))
    cobertura_salud      = Column(String(100))
    embarazo_en_curso    = Column(Boolean,  default=True)
    puerpera             = Column(Boolean,  default=False)
    nombre_establecimiento = Column(String(255))  # Establecimiento de origen (texto)
    nombre_centro_salud  = Column(String(255))     # Centro de Salud asignado (texto)

    # ════════════════════════════════════════════════════════════════════════
    # GRUPO B — Datos del Embarazo
    # ════════════════════════════════════════════════════════════════════════
    fecha_probable_parto        = Column(Date, nullable=False, index=True)  # FPP — parte de la clave natural
    fum                         = Column(Date)
    fecha_diagnostico_embarazo  = Column(Date)
    edad_al_inicio              = Column(Integer)
    numero_embarazo             = Column(Integer)
    eg_actual                   = Column(Float)   # Edad gestacional en semanas y dias
    controles_1er_trim          = Column(SmallInteger)
    controles_2do_trim          = Column(SmallInteger)
    controles_3er_trim          = Column(SmallInteger)
    fecha_ultimo_control        = Column(Date, index=True)
    proxima_cita                = Column(Date)
    riesgo                      = Column(String(50))
    observaciones_riesgo        = Column(Text)      # Campo operativo (no del mapeo, se conserva)
    estado                      = Column(String(50))

    # ════════════════════════════════════════════════════════════════════════
    # GRUPO C — Derivaciones
    # ════════════════════════════════════════════════════════════════════════
    nombre_centro_derivado          = Column(String(255))
    fecha_derivacion                = Column(Date)
    motivo_diagnostico_derivacion   = Column(Text)
    tratamiento                     = Column(Text)
    fin_tipo                        = Column(String(100))
    fin_fecha_gestacion             = Column(Date)

    # ════════════════════════════════════════════════════════════════════════
    # GRUPO D — Contacto y Domicilio
    # ════════════════════════════════════════════════════════════════════════
    telefono               = Column(String(50))
    departamento_domicilio = Column(String(100))
    barrio_paraje_domicilio= Column(String(150))
    calle_domicilio        = Column(String(150))
    nro_puerta_domicilio   = Column(String(20))
    piso_dpto_domicilio    = Column(String(20))
    localidad_domicilio    = Column(String(100))

    # Códigos de establecimiento:
    # sisa_* = Código SISA oficial (POF, WP, Derivaciones)
    # cuie_* = Código Programa SUMAR (solo fuente SUMAR)
    sisa_centro_salud    = Column(String(20), index=True)   # SISA del centro de la paciente
    sisa_centro_derivado = Column(String(20))               # SISA del centro de derivación
    indicaciones         = Column(Text)

    # ════════════════════════════════════════════════════════════════════════
    # GRUPO E — Campos específicos SUMAR
    # ════════════════════════════════════════════════════════════════════════
    cuie_seguimiento             = Column(String(50))	
    afiliado_numero              = Column(String(50))
    afiliado_tipo                = Column(String(50))
    beneficiario_tipo            = Column(String(50))
    programa                     = Column(String(50))
    numero_inscripcion           = Column(String(50))
    fecha_alta_af                = Column(Date)
    nombre_efectora_af           = Column(String(255))
    localidad_afiliacion         = Column(String(100))
    cuie_afiliacion              = Column(String(50))   # CUIE del Programa SUMAR (afiliación)
    prenatal_alta_fecha          = Column(Date)
    prenatal_alta_efector        = Column(String(255))
    prenatal_alta_cuie           = Column(String(50))   # CUIE del Programa SUMAR (alta prenatal)
    prenatal_control_fecha       = Column(Date)
    prenatal_control_efector     = Column(String(255))
    estado_afiliado              = Column(String(50))
    tramo_embarazo               = Column(String(50))
    eg_segun_diagnostico         = Column(Integer)
    fecha_inicio_segun_diagnostico = Column(Date)
    ar_segun_fact_prestacion     = Column(String(50))
    diagnostico_ar_sumar         = Column(Text)
    fecha_prestacion_sumar       = Column(Date)
    cuie_efector_prestacion      = Column(String(50))   # CUIE del Programa SUMAR (prestación)
    codigo_diagnostico           = Column(String(20))
    nombre_efector_prestacion    = Column(String(255))
    localidad_prestacion_sumar   = Column(String(100))
    embarazo_id                  = Column(String(50))
    embarazada_id                = Column(String(50))
    localidad_establecimiento    = Column(String(150))
    fecha_inicio_seguimiento     = Column(Date)
    controles                    = Column(Text)
    tratamientos                 = Column(Text)
    cantidad_controles           = Column(Integer)
    cantidad_tratamientos        = Column(Integer)
    fecha_registro               = Column(DateTime)
    medico_deriva                = Column(String(150))
    medico_recibe                = Column(String(150))

    # ════════════════════════════════════════════════════════════════════════
    # CAPA OPERATIVA — Vínculos RBAC (no provienen de fuentes externas)
    # ════════════════════════════════════════════════════════════════════════
    # cuie_seguimiento: FK al Efector responsable del seguimiento.
    # Se usa para el filtrado RBAC por rol de Centro de Salud.
    # Se puede poblar desde sisa_centro_salud buscando en la tabla efectores_sisa.
    cuie_seguimiento         = Column(String(50), ForeignKey('efectores_sisa.cuie'), index=True)
    derivacion_maternidad_id = Column(Integer,    ForeignKey('maternidades.id'), nullable=True, index=True)

    # ════════════════════════════════════════════════════════════════════════
    # CAPA OPERATIVA — Auditoría ETL y Trazabilidad Multi-Fuente
    # ════════════════════════════════════════════════════════════════════════
    fuente_principal         = Column(String(20))  # Fuente que originó el registro ('pof', 'wp', 'derivaciones', 'sumar')
    fuentes_disponibles      = Column(String(100)) # CSV con fuentes disponibles: 'pof,wp,sumar'
    ultima_actualizacion_pof = Column(DateTime)
    ultima_actualizacion_wp  = Column(DateTime)
    ultima_actualizacion_derivaciones = Column(DateTime)
    ultima_actualizacion_sumar = Column(DateTime)

    fuente                   = Column(String(20))    # 'pof' | 'wp' | 'derivaciones' | 'sumar' (última fuente procesada)
    hoja                     = Column(String(100))   # Nombre de la hoja del Excel fuente
    source_name              = Column(String(255))   # Nombre del archivo fuente
    batch_id                 = Column(String(100))   # ID del lote de carga
    ingestion_at             = Column(DateTime, default=datetime.datetime.utcnow)
    ultimo_contacto_at       = Column(DateTime, nullable=True, index=True)  # Fecha del último contacto registrado
    audit_observations       = Column(Text)          # Notas de calidad / alertas del ETL

    # ── Relación con seguimientos (write-back operativo) ─────────────────
    seguimientos = relationship("Seguimiento", back_populates="paciente", cascade="all, delete-orphan")

    # ── Índices y restricciones ──────────────────────────────────────────
    __table_args__ = (
        # Clave natural: un mismo DNI puede tener múltiples embarazos (FPP distintas)
        Index('idx_dni_fpp', 'dni', 'fecha_probable_parto', unique=True),
        Index('idx_sisa_centro', 'sisa_centro_salud'),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tabla de seguimientos (write-back / novedades operativas)
# ─────────────────────────────────────────────────────────────────────────────

class Seguimiento(Base):
    """
    Registro de contactos y novedades realizadas sobre cada paciente.
    Generado por el personal de salud desde el dashboard (write-back).
    """
    __tablename__ = 'seguimientos'

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    paciente_id        = Column(Integer, ForeignKey('pacientes_gold.id'), nullable=False, index=True)
    fecha_contacto     = Column(DateTime, default=datetime.datetime.utcnow)
    contacto_logrado   = Column(Boolean, default=False)
    medio_contacto     = Column(String(50))  # 'llamada', 'whatsapp', 'sms', 'visita_domiciliaria', 'otro'
    persona_contactada = Column(String(100))  # 'paciente', 'familiar', 'vecino', 'otro'
    telefono_contactado= Column(String(50))
    observaciones      = Column(Text)
    proxima_cita       = Column(Date)
    derivacion_realizada = Column(Boolean, default=False)
    establecimiento_derivacion = Column(String(255))
    personal_salud     = Column(String(100))
    usuario_id         = Column(Integer, ForeignKey('usuarios.id'))
    created_at         = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at         = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    paciente = relationship("PacienteGold", back_populates="seguimientos")
    usuario = relationship("Usuario", backref="seguimientos_realizados")


class PacienteGoldFuentes(Base):
    """
    Tabla de trazabilidad completa del origen de cada dato.
    Permite auditoría fina: saber qué fuente originó cada campo,
    cuándo se actualizó, y detectar inconsistencias entre fuentes.
    """
    __tablename__ = 'pacientes_gold_fuentes'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # FK a paciente consolidado
    paciente_id = Column(Integer, ForeignKey('pacientes_gold.id'), nullable=False, index=True)

    # Identificación de la fuente
    fuente = Column(String(20), nullable=False, index=True)  # 'pof', 'wp', 'derivaciones', 'sumar'
    batch_id = Column(String(100), index=True)  # Lote de ingesta

    # Datos crudos de la fuente (JSON para flexibilidad)
    data_json = Column(Text)  # Registro completo en formato JSON

    # Metadatos de ingesta
    ingestion_at = Column(DateTime, default=datetime.datetime.utcnow)
    source_file = Column(String(255))  # Nombre del archivo o sheet ID
    source_sheet = Column(String(100))  # Nombre de la hoja/sheet

    # Relación
    paciente = relationship("PacienteGold", backref="fuentes_origen")


# Tabla de auditoría de contactos (log histórico)
class ContactoAudit(Base):
    """
    Log histórico de todos los intentos de contacto.
    Se usa para reporting y análisis de efectividad de gestión.
    """
    __tablename__ = 'contactos_audit'

    id = Column(Integer, primary_key=True, autoincrement=True)
    paciente_id = Column(Integer, ForeignKey('pacientes_gold.id'), nullable=False, index=True)
    usuario_id = Column(Integer, ForeignKey('usuarios.id'))

    fecha_intentro = Column(DateTime, default=datetime.datetime.utcnow)
    tipo_intentro = Column(String(50))  # 'llamada', 'whatsapp', 'sms', 'visita'
    resultado = Column(String(50))  # 'logrado', 'fallido', 'sin_respuesta', 'numero_incorrecto'
    observaciones = Column(Text)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    paciente = relationship("PacienteGold")
    usuario = relationship("Usuario")


class PacienteSinDniStage(Base):
    """
    Tabla stage para alojar registros crudos provenientes de fuentes externas
    que carecen de un DNI y no pueden ser incorporados a pacientes_gold.
    """
    __tablename__ = 'pacientes_sin_dni_stage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fuente = Column(String(50))
    hoja = Column(String(100))
    batch_id = Column(String(100), index=True)
    data_json = Column(Text)
    ingestion_at = Column(DateTime, default=datetime.datetime.utcnow)


class VEmbarazosDW(Base):
    """
    Trazabilidad cruda de la extracción desde la vista SQL Server V_EmbarazosDW.
    """
    __tablename__ = 'v_embarazosdw'

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(100), index=True)
    dni = Column(String(20), index=True)
    fecha_probable_parto = Column(Date, index=True)
    controles = Column(Text)
    fecha_registro = Column(DateTime, index=True)
    data_json = Column(Text, nullable=False)
    ingestion_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)


class PacienteSinFppStage(Base):
    """
    Tabla stage para registros con DNI pero sin FPP consolidable
    (y eventualmente sin FUM utilizable), para auditoría y recuperación.
    """
    __tablename__ = 'pacientes_sin_fpp_stage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dni = Column(String(20), index=True)
    fuente = Column(String(50), index=True)
    motivo = Column(String(100), index=True)
    batch_id = Column(String(100), index=True)
    data_json = Column(Text)
    ingestion_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)


class PacienteSinFnacStage(Base):
    """
    Stage de verificación para embarazadas sin fecha de nacimiento válida
    o con edad calculada menor a 10 años al momento de la corrida.
    """
    __tablename__ = 'pacientes_sin_fnac_stage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dni = Column(String(20), index=True)
    fuente = Column(String(50), index=True)
    motivo = Column(String(100), index=True)  # sin_fecha_nacimiento | edad_menor_10
    edad_calculada = Column(Integer)
    batch_id = Column(String(100), index=True)
    data_json = Column(Text)
    ingestion_at = Column(DateTime, default=datetime.datetime.utcnow, index=True)

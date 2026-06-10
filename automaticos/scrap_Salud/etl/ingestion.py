"""
Módulo ETL de ingesta de datos.

Responsabilidades:
- Cargar maestros (Efectores SISA, Maternidades) desde archivos locales.
- Ingestar datos de pacientes desde los 4 archivos Excel fuente.
- Aplicar el mapeo de campos (field_map.py) y deduplicar por (dni, fecha_probable_parto).
- Registrar metadatos de auditoría (fuente, hoja, batch_id, ingestion_at).
"""

import pandas as pd
import os
import uuid
import datetime
import logging
import re
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.database import Efector, Maternidad, Usuario, PacienteGold
from etl.field_map import FIELD_MAP, NATURAL_KEY, ESTABLISHMENT_CODE_TYPE
from werkzeug.security import generate_password_hash

logger = logging.getLogger(__name__)
PARTIAL_DATE_RE = re.compile(r"^\s*(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\s*$")
ISO_DATE_RE = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\s*$")


def _normalize_code(value):
    """Normaliza códigos (CUIE/SISA/INDEC) evitando notación científica."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none"):
        return None
    try:
        if re.fullmatch(r"\d+(\.0+)?", text):
            return str(int(float(text)))
        if "e+" in text.lower():
            return format(int(float(text)), "d")
    except Exception:
        pass
    return text

# ─────────────────────────────────────────────────────────────────────────────
# Maestros
# ─────────────────────────────────────────────────────────────────────────────

def load_local_masters(session: Session):
    """Carga efectores SISA y maternidades desde los archivos CSV/XLSX en /data."""
    efectores_path   = os.path.join("data", "efectores_sisa.xlsx")
    maternidades_path = os.path.join("data", "Maternidades.xlsx")

    if os.path.exists(efectores_path):
        logger.info(f"Cargando efectores desde {efectores_path}...")
        df = pd.read_excel(efectores_path, sheet_name="efectores_sisa")
        loaded = 0
        for _, row in df.iterrows():
            cuie_val = _normalize_code(row.get('CUIE'))
            if not cuie_val:
                continue
            codigo_sisa = _normalize_code(row.get('Codigo SISA'))
            existing = session.query(Efector).filter_by(cuie=cuie_val).first()
            if existing:
                existing.codigo_sisa = codigo_sisa
                existing.nombre = str(row.get('NOMBRE', '')).strip() or existing.nombre
                existing.dependencia = str(row.get('DEPENDENCIA ADMINISTRATIVA', '')).strip() or existing.dependencia
                existing.codigo_indec_provincia = _normalize_code(row.get('CODIGO INDEC PROVINCIA'))
                existing.codigo_indec_departamento = _normalize_code(row.get('CODIGO INDEC DEPARTAMENTO'))
                existing.departamento = str(row.get('DEPARTAMENTO', '')).strip() or existing.departamento
                existing.codigo_indec_localidad = _normalize_code(row.get('CODIGO INDEC LOCALIDAD'))
                existing.localidad = str(row.get('LOCALIDAD', '')).strip() or existing.localidad
                existing.ciudad = str(row.get('CIUDAD', '')).strip() or existing.ciudad
                existing.domicilio = str(row.get('DOMICILIO', '')).strip() or existing.domicilio
                existing.latitud = _normalize_code(row.get('LATITUD'))
                existing.longitud = _normalize_code(row.get('LONGITUD'))
            else:
                session.add(Efector(
                    cuie=cuie_val,
                    codigo_sisa=codigo_sisa,
                    nombre=str(row.get('NOMBRE', '')).strip(),
                    dependencia=str(row.get('DEPENDENCIA ADMINISTRATIVA', '')).strip() or None,
                    codigo_indec_provincia=_normalize_code(row.get('CODIGO INDEC PROVINCIA')),
                    codigo_indec_departamento=_normalize_code(row.get('CODIGO INDEC DEPARTAMENTO')),
                    departamento=str(row.get('DEPARTAMENTO', '')).strip() or None,
                    codigo_indec_localidad=_normalize_code(row.get('CODIGO INDEC LOCALIDAD')),
                    localidad=str(row.get('LOCALIDAD', '')).strip() or None,
                    ciudad=str(row.get('CIUDAD', '')).strip() or None,
                    domicilio=str(row.get('DOMICILIO', '')).strip() or None,
                    latitud=_normalize_code(row.get('LATITUD')),
                    longitud=_normalize_code(row.get('LONGITUD')),
                ))
                loaded += 1
        session.commit()
        logger.info(f"  → {loaded} efectores cargados.")

    if os.path.exists(maternidades_path):
        logger.info(f"Cargando maternidades desde {maternidades_path}...")
        df = pd.read_excel(maternidades_path, sheet_name="Maternidades")
        loaded = 0
        for _, row in df.iterrows():
            codigo_sisa = _normalize_code(row.get("Codigo Sisa"))
            cuie = _normalize_code(row.get("CUIE"))
            nombre_val = _clean_str(row.get("Maternidad"), max_len=255)
            categoria = _clean_str(row.get("Categoria"), max_len=50)
            if not (codigo_sisa or cuie or nombre_val):
                continue

            # 1) Match canónico por codigo_sisa/cuie
            existing = None
            if codigo_sisa:
                existing = session.query(Maternidad).filter_by(codigo_sisa=codigo_sisa).first()
            if existing is None and cuie:
                existing = session.query(Maternidad).filter_by(cuie=cuie).first()

            # 2) Reparación de cargas antiguas: nombre=codigo_sisa y localidad=cuie
            if existing is None and codigo_sisa and cuie:
                existing = session.query(Maternidad).filter_by(nombre=codigo_sisa, localidad=cuie).first()

            if existing:
                existing.codigo_sisa = codigo_sisa or existing.codigo_sisa
                existing.cuie = cuie or existing.cuie
                existing.nombre = nombre_val or existing.nombre
                existing.categoria = categoria or existing.categoria
                existing.nivel_complejidad = categoria or existing.nivel_complejidad
                # Localidad en maternidades no se usa como fuente canónica.
                # Se deja vacía para evitar mezclarla con CUIE.
                if existing.localidad and cuie and str(existing.localidad).strip() == str(cuie).strip():
                    existing.localidad = None
            else:
                session.add(Maternidad(
                    codigo_sisa=codigo_sisa,
                    cuie=cuie,
                    nombre=nombre_val or (codigo_sisa or cuie),
                    categoria=categoria,
                    nivel_complejidad=categoria,
                    localidad=None,
                ))
                loaded += 1
        session.commit()
        logger.info(f"  → {loaded} maternidades cargadas.")


def create_initial_users(session: Session):
    """Crea los usuarios iniciales del sistema (Admin y Coordinador)."""
    usuarios_iniciales = [
        ("admin", "admin123", "Administrador"),
        ("coord", "coord123", "Coordinador"),
    ]
    for username, password, role in usuarios_iniciales:
        if not session.query(Usuario).filter_by(username=username).first():
            session.add(Usuario(
                username      = username,
                password_hash = generate_password_hash(password),
                role          = role,
            ))
            logger.info(f"Usuario creado: {username} / {password} [{role}]")
    session.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Ingesta de fuentes Excel
# ─────────────────────────────────────────────────────────────────────────────

def _clean_date(val):
    """Intenta convertir un valor a date y completa el año actual cuando falta."""
    if pd.isna(val) or val == '' or val is None:
        return None

    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val

    text = str(val).strip()
    if ISO_DATE_RE.match(text):
        try:
            return datetime.date.fromisoformat(text)
        except ValueError:
            return None

    match = PARTIAL_DATE_RE.match(text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_token = match.group(3)
        if year_token is None:
            year = datetime.date.today().year
        else:
            year = int(year_token)
            if len(year_token) == 2:
                year += 2000

        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None

    try:
        parsed = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.date()
    except Exception:
        return None


def _clean_str(val, max_len=None):
    """Limpia y trunca un string. Retorna None si está vacío."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('nan', 'none', ''):
        return None
    return s[:max_len] if max_len else s


def _clean_int(val):
    """Convierte a entero. Retorna None si falla."""
    try:
        f = float(val)
        return int(f) if not pd.isna(f) else None
    except Exception:
        return None

def _clean_float(val):
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().lower()
    if not s or s in ('nan', 'none', ''):
        return None
    match = re.search(r"(\d+[\.,]\d+|\d+)", s)
    if match:
        num_str = match.group(1).replace(',', '.')
        try:
            return float(num_str)
        except Exception:
            pass
    return None


def load_excel_source(
    session:   Session,
    file_path: str,
    sheet:     str,
    fuente:    str,          # 'pof' | 'wp' | 'derivaciones' | 'sumar'
    batch_id:  str = None,
) -> dict:
    """
    Carga un archivo Excel a la tabla pacientes_gold aplicando el mapeo de campos.

    Parámetros:
        session   : Sesión SQLAlchemy activa.
        file_path : Ruta al archivo Excel.
        sheet     : Nombre de la hoja a leer.
        fuente    : Clave de fuente (debe existir en FIELD_MAP).
        batch_id  : Identificador del lote. Se genera automáticamente si no se provee.

    Retorna:
        dict con contadores: {'insertados': n, 'actualizados': n, 'errores': n}
    """
    if fuente not in FIELD_MAP:
        raise ValueError(f"Fuente '{fuente}' no definida en field_map.py")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

    batch_id    = batch_id or str(uuid.uuid4())[:8]
    field_map   = FIELD_MAP[fuente]
    code_type   = ESTABLISHMENT_CODE_TYPE[fuente]  # 'sisa' o 'cuie'
    ingestion_at = datetime.datetime.utcnow()

    counters = {"insertados": 0, "actualizados": 0, "errores": 0}

    logger.info(f"Leyendo [{fuente.upper()}] {file_path} → hoja '{sheet}'...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
    except Exception as e:
        logger.error(f"Error al leer el archivo: {e}")
        return counters

    # Renombrar columnas según el mapeo
    df = df.rename(columns={k: v for k, v in field_map.items() if k in df.columns})
    logger.info(f"  Filas leídas: {len(df)}  |  Columnas mapeadas: {len(field_map)}")

    # Columnas Gold disponibles
    gold_cols = {c.name for c in PacienteGold.__table__.columns}

    for idx, row in df.iterrows():
        try:
            # ── Clave natural ────────────────────────────────────────────
            dni = _clean_str(row.get("dni"), 20)
            fpp = _clean_date(row.get("fecha_probable_parto"))

            if not dni or not fpp:
                counters["errores"] += 1
                logger.warning(f"  Fila {idx}: DNI o FPP nulos — omitida.")
                continue

            # ── Construir dict de valores ────────────────────────────────
            data = {}
            for col in gold_cols:
                if col in ("id", "ingestion_at"):
                    continue
                val = row.get(col)
                if val is None:
                    continue

                # Tipo de dato según la columna
                if col in (
                    "fecha_nacimiento", "fecha_probable_parto", "fum",
                    "fecha_diagnostico_embarazo", "fecha_ultimo_control",
                    "proxima_cita", "fecha_derivacion", "fin_fecha_gestacion",
                    "fecha_alta_af", "prenatal_alta_fecha", "prenatal_control_fecha",
                    "fecha_prestacion_sumar", "fecha_inicio_segun_diagnostico",
                ):
                    cleaned = _clean_date(val)
                elif col in (
                    "edad_actual", "edad_al_inicio", "numero_embarazo",
                    "controles_1er_trim", "controles_2do_trim", "controles_3er_trim",
                    "derivacion_maternidad_id",
                ):
                    cleaned = _clean_int(val)
                elif col in ("eg_actual", "eg_segun_diagnostico"):
                    cleaned = _clean_float(val)
                elif col in ("embarazo_en_curso", "puerpera"):
                    s = _clean_str(val)
                    cleaned = True if s and s.lower() in ("1", "si", "sí", "true", "s") else False
                else:
                    # Determinar max_len según la columna
                    col_obj = PacienteGold.__table__.columns.get(col)
                    max_len = None
                    if col_obj is not None and hasattr(col_obj.type, 'length'):
                        max_len = col_obj.type.length
                    cleaned = _clean_str(val, max_len)

                if cleaned is not None:
                    data[col] = cleaned

            # ── Metadatos de auditoría ETL ───────────────────────────────
            data.update({
                "dni":                  dni,
                "fecha_probable_parto": fpp,
                "fuente":               fuente,
                "hoja":                 sheet[:100],
                "source_name":          os.path.basename(file_path)[:255],
                "batch_id":             batch_id,
                "ingestion_at":         ingestion_at,
            })

            # ── Deduplicación: INSERT o UPDATE ────────────────────────────
            existing = session.query(PacienteGold).filter_by(
                dni                  = dni,
                fecha_probable_parto = fpp,
            ).first()

            if existing:
                # Actualizar solo campos no nulos (no sobreescribir con nulos)
                for k, v in data.items():
                    if k not in ("id", "ingestion_at") and v is not None:
                        setattr(existing, k, v)
                counters["actualizados"] += 1
            else:
                session.add(PacienteGold(**data))
                counters["insertados"] += 1

            # Commit cada 500 filas para no saturar la transacción
            if (idx + 1) % 500 == 0:
                session.commit()
                logger.info(f"  ... {idx + 1} filas procesadas.")

        except Exception as e:
            logger.warning(f"  Fila {idx} con error: {e}")
            counters["errores"] += 1
            session.rollback()

    session.commit()
    logger.info(
        f"  Resultado [{fuente.upper()}]: "
        f"+{counters['insertados']} nuevos | "
        f"~{counters['actualizados']} actualizados | "
        f"✗{counters['errores']} errores"
    )
    return counters

"""
Orquestador principal del ETL con priorización de fuentes.

Prioridad para mismos DNI (según Excel de mapeo):
1. POF (1ra prioridad - máxima)
2. WP (2da prioridad)
3. SUMAR (3ra prioridad)
4. Derivaciones (4ta prioridad - mínima)

Los campos comunes se consolidan según prioridad.
Los campos exclusivos se importan tal cual desde cada fuente.

Uso:
    python etl/orchestrator.py
    python etl/orchestrator.py --mock  # Para testing sin conexión real
"""

import os
import sys
import json
import uuid
import datetime
import logging
import re
import unicodedata
from dotenv import load_dotenv
from app.db_config import get_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Imports locales
from models.database import (
    PacienteGold,
    PacienteGoldFuentes,
    Efector,
    Maternidad,
    PacienteSinDniStage,
    PacienteSinFppStage,
    PacienteSinFnacStage,
    VEmbarazosDW,
)
from etl.field_map import FIELD_MAP, FUENTE_PRIORIDADES, NATURAL_KEY
from etl.google_sheets_loader import GoogleSheetsLoader, MockGoogleSheetsLoader, SOURCES_CONFIG
from etl.google_drive_loader import GoogleDriveExcelLoader
from etl.external_db_loader import ExternalDBLoader, MockExternalDBLoader

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

DB_URL = get_database_url()
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACIENTE_GOLD_COLUMNS = {column.name for column in PacienteGold.__table__.columns}
PARTIAL_DATE_RE = re.compile(r"^\s*(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\s*$")
ISO_DATE_RE = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\s*$")
SQL_DATETIME_RE = re.compile(
    r"^\s*\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)?\s*$"
)
ALIAS_EFECTORES_PATH = os.path.join(BASE_DIR, "data", "mapeo_efectores_alias.xlsx")
ALIAS_DERIVA = {}
ALIAS_RECIBE = {}
UPPERCASE_FIELDS = {
    "apellido",
    "nombre",
    "riesgo",
    "observaciones_riesgo",
    "motivo_diagnostico_derivacion",
    "medico_deriva",
    "medico_recibe",
    "calle_domicilio",
    "nro_puerta_domicilio",
    "barrio_paraje_domicilio",
    "localidad_domicilio",
    "controles",
    "tratamientos",
}


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES DE LIMPIEZA
# ─────────────────────────────────────────────────────────────────────────────

def _clean_date(val):
    """Convierte valor a date y completa el año actual cuando no viene informado."""
    if pd.isna(val) or val == '' or val is None:
        return None

    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val

    # Excel serial date
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            parsed_excel = pd.to_datetime(val, unit="D", origin="1899-12-30", errors="coerce")
            if not pd.isna(parsed_excel):
                return parsed_excel.date()
        except Exception:
            pass

    text = str(val).strip()
    text = text.replace("|/", "/").replace("|-", "-")
    if re.match(r"^[/-]\d{1,2}[/-]\d{2,4}$", text):
        text = f"1{text}"
    text = text.replace('|', '1')
    if ISO_DATE_RE.match(text):
        try:
            return datetime.date.fromisoformat(text)
        except ValueError:
            return None

    if SQL_DATETIME_RE.match(text):
        parsed_sql = pd.to_datetime(text, dayfirst=False, errors="coerce")
        if not pd.isna(parsed_sql):
            return parsed_sql.date()

    match = PARTIAL_DATE_RE.match(text)
    if match:
        num1 = int(match.group(1))
        num2 = int(match.group(2))
        year_token = match.group(3)

        day = num1
        month = num2

        if year_token is None:
            year = datetime.date.today().year
        else:
            year = int(year_token)
            if len(year_token) == 2:
                current_year = datetime.date.today().year
                if year <= (current_year - 2000) + 50:
                    year += 2000
                else:
                    year += 1900

        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None

    # Fallback: primero DD/MM, luego parseo general
    for dayfirst in (True, False):
        try:
            parsed = pd.to_datetime(text, dayfirst=dayfirst, errors='coerce')
            if not pd.isna(parsed):
                return parsed.date()
        except Exception:
            continue
    return None


def _clean_str(val, max_len=255):
    """Limpia y trunca string. Retorna None si vacío."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('nan', 'none', '<na>', '<nat>', ''):
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
    """Convierte a float extrayendo el primer número. Retorna None si falla."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().lower()
    if not s or s in ('nan', 'none', ''):
        return None
    
    # Extraer primer numero decial o entero
    match = re.search(r"(\d+[\.,]\d+|\d+)", s)
    if match:
        num_str = match.group(1).replace(',', '.')
        try:
            return float(num_str)
        except Exception:
            pass
    return None


def _parse_gestational_age_weeks(val):
    """
    Normaliza edad gestacional en semanas.

    Soporta:
    - '34 sem'
    - '34-36 sem' (toma la menor)
    - '34/36 sem' (toma la menor)
    - 'puerpera' / 'puerpers' (sin edad gestacional activa)
    """
    if pd.isna(val) or val is None:
        return None, False

    raw = str(val).strip().lower()
    if not raw or raw in ('nan', 'none', ''):
        return None, False

    if 'puerper' in raw:
        return None, True

    normalized = raw.replace(',', '.')
    numbers = re.findall(r"\d+(?:\.\d+)?", normalized)
    if not numbers:
        return None, False

    try:
        weeks = min(float(n) for n in numbers)
    except Exception:
        return None, False

    return weeks, False


def _clean_bool(val):
    """Convierte a booleano."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().lower()
    if s in ('1', 'si', 'sí', 'true', 's', 'v'):
        return True
    if s in ('0', 'no', 'false', 'n', 'f'):
        return False
    return None


def _has_value(val) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    return str(val).strip().lower() not in ("", "nan", "none", "null", "<na>", "<nat>")


def _estimate_fpp_from_fum(fum: datetime.date | None) -> datetime.date | None:
    if not fum:
        return None
    return fum + datetime.timedelta(days=280)


def _calculate_age_years(fecha_nacimiento: datetime.date | None, ref_date: datetime.date | None = None) -> int | None:
    """
    Equivalente a EXTRACT(YEAR FROM age(CURRENT_DATE, fecha_nacimiento)).
    """
    if not fecha_nacimiento:
        return None
    today = ref_date or datetime.date.today()
    years = today.year - fecha_nacimiento.year
    if (today.month, today.day) < (fecha_nacimiento.month, fecha_nacimiento.day):
        years -= 1
    return years


def _calculate_eg_actual_from_fpp(
    fecha_probable_parto: datetime.date | None,
    ref_date: datetime.date | None = None,
) -> float | None:
    """
    Recalcula EG actual para todas las fuentes a partir de la FPP consolidada.

    Fórmula de negocio:
        EG = 40 - ((FPP - fecha_hoy) / 7)

    Formato almacenado:
        semanas.dias  (días en base 7, de 0 a 6)
    Ejemplos:
        25.6  -> 25 semanas y 6 días
        al día siguiente -> 26.0
    """
    if not fecha_probable_parto:
        return None

    today = ref_date or datetime.date.today()
    days_to_fpp = (fecha_probable_parto - today).days
    gestation_days = 280 - days_to_fpp

    # Representación clínica: semanas + días (días 0..6), no decimal base 10.
    if gestation_days >= 0:
        weeks, days = divmod(gestation_days, 7)
        return float(f"{int(weeks)}.{int(days)}")

    weeks, days = divmod(abs(gestation_days), 7)
    return -float(f"{int(weeks)}.{int(days)}")


def _date_value(val) -> datetime.date | None:
    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val
    return None


def _fin_fecha_corresponde_embarazo_actual(data: dict) -> bool:
    """
    Verifica si fin_fecha_gestacion pertenece al embarazo actual según FPP.

    Se usa la ventana estimada FPP - 280 días hasta FPP + 21 días para evitar
    cerrar embarazos actuales por fechas cargadas de gestaciones previas.
    """
    fin_fecha = _date_value(data.get("fin_fecha_gestacion"))
    fpp = _date_value(data.get("fecha_probable_parto"))
    if not fin_fecha or not fpp:
        return False

    inicio_estimado = fpp - datetime.timedelta(days=280)
    fin_tolerado = fpp + datetime.timedelta(days=21)
    return inicio_estimado <= fin_fecha <= fin_tolerado


def _should_recalculate_eg(data: dict, ref_date: datetime.date | None = None) -> bool:
    """
    Define si corresponde recalcular eg_actual para la corrida actual.

    Recalcular solo en embarazos en curso:
    - Si embarazo_en_curso=False -> no recalcular
    - Si puerpera=True -> no recalcular
    - Si hay fin de gestación anterior a hoy del embarazo actual -> no recalcular
    - Si FPP < fecha de corrida -> no recalcular
    - Si embarazo_en_curso=True y FPP >= fecha de corrida -> recalcular
    - Si no hay flag explícito, usar FPP como fallback.
    """
    if data.get("embarazo_en_curso") is False:
        return False
    if data.get("puerpera") is True:
        return False

    fpp = data.get("fecha_probable_parto")
    if not isinstance(fpp, datetime.date):
        return False

    today = ref_date or datetime.date.today()
    fin_fecha = _date_value(data.get("fin_fecha_gestacion"))
    if fin_fecha and fin_fecha < today and _fin_fecha_corresponde_embarazo_actual(data):
        return False

    if fpp < today:
        return False

    if data.get("embarazo_en_curso") is True:
        return True

    return True


def enforce_embarazo_en_curso_rules(data: dict, ref_date: datetime.date | None = None) -> dict:
    """
    Regla de consistencia:
    - Si puerpera=True, embarazo_en_curso debe quedar False.
    - Si fin_fecha_gestacion es anterior a la fecha de corrida y corresponde
      al embarazo actual según FPP, embarazo_en_curso debe quedar False.
    """
    if data.get("puerpera") is True:
        data["embarazo_en_curso"] = False
        return data

    fin_fecha = _date_value(data.get("fin_fecha_gestacion"))
    today = ref_date or datetime.date.today()
    if fin_fecha and fin_fecha < today and _fin_fecha_corresponde_embarazo_actual(data):
        data["embarazo_en_curso"] = False
    return data


def _stage_fnac_issue(session, data: dict, batch_id: str, motivo: str, edad_calculada: int | None):
    session.add(
        PacienteSinFnacStage(
            dni=str(data.get("dni")) if data.get("dni") else None,
            fuente=data.get("fuente_principal") or data.get("fuente") or "desconocida",
            motivo=motivo,
            edad_calculada=edad_calculada,
            batch_id=batch_id,
            data_json=json.dumps({k: str(v) for k, v in data.items() if v is not None}, default=str),
            ingestion_at=datetime.datetime.now(datetime.timezone.utc),
        )
    )


def _stage_eg_issue(session, data: dict, batch_id: str, eg_actual: float | None):
    """
    Envía a stage registros con EG actual menor a 2 semanas.
    """
    session.add(
        PacienteSinFppStage(
            dni=str(data.get("dni")) if data.get("dni") else None,
            fuente=data.get("fuente_principal") or data.get("fuente") or "desconocida",
            motivo="eg_actual_menor_2",
            batch_id=batch_id,
            data_json=json.dumps({k: str(v) for k, v in data.items() if v is not None}, default=str),
            ingestion_at=datetime.datetime.now(datetime.timezone.utc),
        )
    )
    logger.warning(
        f"  OMITIDO por EG<2 semanas: DNI={data.get('dni')}, "
        f"FPP={data.get('fecha_probable_parto')}, EG={eg_actual}"
    )


def normalize_uppercase_fields(data: dict) -> dict:
    """
    Convierte a mayúsculas campos textuales definidos para almacenamiento en gold.
    """
    for field in UPPERCASE_FIELDS:
        val = data.get(field)
        if isinstance(val, str):
            data[field] = val.upper()
    return data


def _normalize_alias_key(val) -> str:
    if val is None or pd.isna(val):
        return ""
    txt = str(val).strip().upper()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _normalize_colname(val) -> str:
    if val is None:
        return ""
    txt = str(val).strip().upper()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _normalize_sisa_code(val) -> str | None:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    try:
        if re.fullmatch(r"\d+(\.0+)?", s):
            return str(int(float(s)))
        if "e+" in s.lower():
            return str(int(float(s)))
    except Exception:
        pass
    return s


def _load_alias_maps():
    global ALIAS_DERIVA, ALIAS_RECIBE
    if ALIAS_DERIVA or ALIAS_RECIBE:
        return
    if not os.path.exists(ALIAS_EFECTORES_PATH):
        logger.warning(f"No se encontró archivo de alias: {ALIAS_EFECTORES_PATH}")
        return
    try:
        df_deriva = pd.read_excel(ALIAS_EFECTORES_PATH, sheet_name="mapeo_centro_deriva")
        for _, r in df_deriva.iterrows():
            k = _normalize_alias_key(r.get("EFECTOR que deriva"))
            if not k:
                continue
            ALIAS_DERIVA[k] = {
                "nombre_establecimiento": _clean_str(r.get("nombre_establecimiento"), max_len=255),
                "sisa_centro_salud": _normalize_sisa_code(r.get("sisa_centro_salud")),
            }

        df_recibe = pd.read_excel(ALIAS_EFECTORES_PATH, sheet_name="mapeo_centro_recibe")
        for _, r in df_recibe.iterrows():
            k = _normalize_alias_key(r.get("MATERNIDAD que recibe"))
            if not k:
                continue
            ALIAS_RECIBE[k] = {
                "nombre_centro_derivado": _clean_str(r.get("nombre_centro_derivado"), max_len=255),
                "sisa_centro_derivado": _normalize_sisa_code(r.get("sisa_centro_derivado")),
            }
    except Exception as exc:
        logger.warning(f"No se pudieron cargar alias de efectores: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CARGA DE FUENTES DESDE GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────

def load_google_source(fuente: str) -> pd.DataFrame:
    """
    Carga datos desde Google Sheets para una fuente específica.

    Args:
        fuente: Clave de fuente ('pof', 'wp', 'derivaciones', 'sumar')

    Returns:
        DataFrame con los datos crudos de la fuente
    """
    logger.info(f"Cargando fuente desde Google Sheets: {fuente.upper()}")

    config = SOURCES_CONFIG.get(fuente)
    if not config:
        raise ValueError(f"Fuente '{fuente}' no configurada en SOURCES_CONFIG")

    credentials_path = os.path.join(BASE_DIR, config["credentials"])
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credenciales no encontradas: {credentials_path}")

    source_kind = config.get("source_kind", "google_sheets")
    if source_kind == "drive_excel":
        loader = GoogleDriveExcelLoader(credentials_path)
    else:
        loader = GoogleSheetsLoader(credentials_path)

    sheet_names = config.get("sheet_names")
    if sheet_names:
        return loader.load_multiple_sheets(
            config["sheet_id"],
            sheet_names,
            header_rows=config.get("header_rows"),
            start_col=config.get("start_col", 0),
        )
    return loader.load_sheet(
        config["sheet_id"],
        config["sheet_name"],
        header_row=config.get("header_rows", {}).get(config["sheet_name"], 1),
        start_col=config.get("start_col", 0),
    )


def load_vista_source(use_mock: bool = False) -> pd.DataFrame:
    if use_mock:
        return MockExternalDBLoader().load_view("V_EmbarazosDW")

    loader = ExternalDBLoader(
        host=os.getenv("MSSQL_HOST"),
        database=os.getenv("MSSQL_DB"),
        user=os.getenv("MSSQL_USER"),
        password=os.getenv("MSSQL_PASSWORD"),
        port=int(os.getenv("MSSQL_PORT", "1433")),
        driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        encrypt=os.getenv("MSSQL_ENCRYPT", "no"),
    )
    view_name = os.getenv("MSSQL_VIEW_NAME", "V_EmbarazosDW")
    where_clause = (
        "(FechaProbableParto >= '2026-01-01' OR FUM >= '2025-02-01')"
    )
    return loader.load_view(view_name, where_clause=where_clause)


def persist_vista_raw(session, df: pd.DataFrame, batch_id: str):
    now = datetime.datetime.now(datetime.timezone.utc)
    for _, row in df.iterrows():
        raw = row.to_dict()
        fecha_registro = pd.to_datetime(row.get("Reg F.H"), dayfirst=True, errors="coerce")
        session.add(
            VEmbarazosDW(
                batch_id=batch_id,
                dni=_clean_str(row.get("DNI"), max_len=20),
                fecha_probable_parto=_clean_date(row.get("FechaProbableParto")),
                controles=_clean_str(row.get("Controles"), max_len=None),
                fecha_registro=fecha_registro.to_pydatetime() if not pd.isna(fecha_registro) else None,
                data_json=json.dumps({k: str(v) for k, v in raw.items() if v is not None}, default=str),
                ingestion_at=now,
            )
        )


def dedupe_vista(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup por (DNI, FechaProbableParto).
    Regla: primero registros con controles; luego fecha_registro más reciente.
    """
    if df.empty:
        return df

    work = df.copy()
    work["__dni_key"] = work["DNI"].astype(str).str.strip()
    work["__fpp_key"] = pd.to_datetime(work["FechaProbableParto"], errors="coerce")
    work["__has_controles"] = work["Controles"].apply(_has_value).astype(int)
    work["__fecha_registro"] = pd.to_datetime(work["Reg F.H"], dayfirst=True, errors="coerce")

    work = work[work["__dni_key"].notna() & work["__fpp_key"].notna()]
    work = work.sort_values(
        by=["__dni_key", "__fpp_key", "__has_controles", "__fecha_registro"],
        ascending=[True, True, False, False],
        na_position="last",
    )
    best = work.drop_duplicates(subset=["__dni_key", "__fpp_key"], keep="first")
    return best.drop(columns=["__dni_key", "__fpp_key", "__has_controles", "__fecha_registro"], errors="ignore")


# ─────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE FILAS
# ─────────────────────────────────────────────────────────────────────────────

def process_row(row: pd.Series, fuente: str) -> dict:
    """
    Procesa una fila aplicando el mapeo de campos.

    Args:
        row: Fila del DataFrame
        fuente: Clave de la fuente

    Returns:
        Diccionario con campos mapeados y limpios
    """
    field_map = FIELD_MAP.get(fuente, {})
    data = {}
    row_col_index = {_normalize_colname(col): col for col in row.index}
    # Aplicar mapeo
    for col_origen, col_destino in field_map.items():
        if col_destino not in PACIENTE_GOLD_COLUMNS:
            continue
        actual_col = col_origen if col_origen in row.index else row_col_index.get(_normalize_colname(col_origen))
        if actual_col:
            val = row[actual_col]
            if val is not None and not pd.isna(val):
                # Determinar tipo de limpieza según campo
                if any(x in col_destino for x in ['fecha', 'fum', 'cita']):
                    data[col_destino] = _clean_date(val)
                elif col_destino in ['edad_actual', 'edad_al_inicio', 'numero_embarazo', 'cantidad_controles', 'cantidad_tratamientos', 'derivacion_maternidad_id']:
                    data[col_destino] = _clean_int(val)
                elif col_destino in ['eg_actual', 'eg_segun_diagnostico']:
                    weeks, is_puerpera = _parse_gestational_age_weeks(val)
                    data[col_destino] = weeks
                    if is_puerpera:
                        data['puerpera'] = True
                        data['embarazo_en_curso'] = False
                elif col_destino in ['embarazo_en_curso', 'puerpera', 'activo']:
                    data[col_destino] = _clean_bool(val)
                else:
                    data[col_destino] = _clean_str(val)

    # Lógica de separación Apellido y Nombre (para POF, WP, Derivaciones)
    if 'apellido' in data and 'nombre' not in data:
        full_name = str(data['apellido']).strip()
        if full_name and full_name.lower() not in ('nan', 'none'):
            if ',' in full_name:
                parts = full_name.split(',', 1)
                data['apellido'] = parts[0].strip()
                data['nombre'] = parts[1].strip()
            elif ' ' in full_name:
                parts = full_name.split(' ', 1)
                data['apellido'] = parts[0].strip()
                data['nombre'] = parts[1].strip()

    # Regla de negocio SUMAR:
    # Si AR_Segun_Fact_Prest indica "Alto Riesgo", riesgo debe ser "SI".
    if fuente == "sumar":
        ar_val = row.get("AR_Segun_Fact_Prest")
        ar_text = str(ar_val).strip().lower() if ar_val is not None and not pd.isna(ar_val) else ""
        if "alto riesgo" in ar_text:
            data["riesgo"] = "SI"

    # Regla de negocio DERIVACIONES: siempre riesgo = SI.
    if fuente == "derivaciones":
        _load_alias_maps()
        data["riesgo"] = "SI"
        deriva_key = _normalize_alias_key(row.get("EFECTOR que deriva"))
        recibe_key = _normalize_alias_key(row.get("MATERNIDAD que recibe"))

        deriva = ALIAS_DERIVA.get(deriva_key)
        if deriva:
            if deriva.get("nombre_establecimiento"):
                data["nombre_establecimiento"] = deriva["nombre_establecimiento"]
            if deriva.get("sisa_centro_salud"):
                data["sisa_centro_salud"] = deriva["sisa_centro_salud"]

        recibe = ALIAS_RECIBE.get(recibe_key)
        if recibe:
            if recibe.get("nombre_centro_derivado"):
                data["nombre_centro_derivado"] = recibe["nombre_centro_derivado"]
            if recibe.get("sisa_centro_derivado"):
                data["sisa_centro_derivado"] = recibe["sisa_centro_derivado"]

        # Nota: no se copia sisa_centro_salud a sisa_centro_derivado como fallback.
        # Todas las maternidades de la red perinatal tienen código SISA propio
        # en mapeo_centro_recibe. Si falta, indica un alias no cargado en el Excel.

    if fuente == "alto_riesgo_caps":
        _load_alias_maps()
        data["riesgo"] = "SI"

        # Concatenar observaciones en orden pedido con barra vertical.
        obs_parts = []
        for col in [
            "FP/TPO",
            "SEGUIMIENTO",
            "OBSERVACIONES",
            "RN",
            "ANTICONCEPCION",
        ]:
            if col in row.index and _has_value(row.get(col)):
                obs_parts.append(str(row.get(col)).strip())
        combined_col = "FP/TPO + SEGUIMIENTO + OBSERVACIONES    /   RN +  ANTICONCEPCION"
        if not obs_parts and combined_col in row.index and _has_value(row.get(combined_col)):
            obs_parts.append(str(row.get(combined_col)).strip())
        if obs_parts:
            data["observaciones_riesgo"] = " | ".join(obs_parts)

        # FPP estimada a partir de fecha derivación y EG
        if not data.get("fecha_probable_parto"):
            data["fecha_probable_parto"] = _estimate_fpp_from_diagnosis(data)

        # Normalización de origen por CAPS (número)
        caps_key = _normalize_alias_key(row.get("CAPS"))
        deriva = ALIAS_DERIVA.get(caps_key)
        if deriva:
            if deriva.get("nombre_establecimiento"):
                data["nombre_establecimiento"] = deriva["nombre_establecimiento"]
            if deriva.get("sisa_centro_salud"):
                data["sisa_centro_salud"] = deriva["sisa_centro_salud"]

        # Normalización de destino por alias de maternidad.
        # Si no viene en columna explícita, usar nombre de hoja.
        destino_raw = row.get("Nombre Hoja Sheet (HOSPITAL VIDAL / HOSPITAL LLANO)") or row.get("_source_sheet")
        recibe_key = _normalize_alias_key(destino_raw)
        recibe = ALIAS_RECIBE.get(recibe_key)
        if recibe:
            if recibe.get("nombre_centro_derivado"):
                data["nombre_centro_derivado"] = recibe["nombre_centro_derivado"]
            if recibe.get("sisa_centro_derivado"):
                data["sisa_centro_derivado"] = recibe["sisa_centro_derivado"]

        # Si falta mapeo crítico de alias/SISA, registrar en stage como pendiente
        if not data.get("sisa_centro_salud") or not data.get("sisa_centro_derivado"):
            data["_stage_motivo"] = "alias_caps_o_destino_no_mapeado"

    # Regla vista DW:
    # si falta FPP y hay FUM válida, estimar FPP a 40 semanas.
    if fuente == "v_embarazosdw" and not data.get("fecha_probable_parto"):
        data["fecha_probable_parto"] = _estimate_fpp_from_fum(data.get("fum"))

    return data


# ─────────────────────────────────────────────────────────────────────────────
# CONSOLIDACIÓN CON PRIORIDADES
# ─────────────────────────────────────────────────────────────────────────────

def consolidate_patient_data(registros_por_fuente: dict) -> dict:
    """
    Consolida múltiples registros de un mismo paciente según prioridad de fuentes.

    Args:
        registros_por_fuente: Dict {fuente: {campo: valor}}

    Returns:
        Dict consolidado con campos de mayor prioridad
    """
    consolidado = {}
    fuentes_disponibles = []

    # Ordenar fuentes por prioridad (menor número = más prioridad)
    fuentes_ordenadas = sorted(
        registros_por_fuente.keys(),
        key=lambda f: FUENTE_PRIORIDADES.get(f, 99)
    )

    for fuente in fuentes_ordenadas:
        fuentes_disponibles.append(fuente)
        datos_fuente = registros_por_fuente[fuente]

        # Para cada campo, si no existe en consolidado, lo agrega
        # (las fuentes de mayor prioridad ya lo habrían puesto)
        for campo, valor in datos_fuente.items():
            if valor is not None and campo not in consolidado:
                consolidado[campo] = valor

            # Si la fuente tiene mayor prioridad, sobreescribe campos comunes
            if campo in ['dni', 'apellido', 'nombre', 'fecha_nacimiento',
                         'fecha_probable_parto', 'riesgo', 'fecha_ultimo_control']:
                prioridad_actual = FUENTE_PRIORIDADES.get(
                    next((f for f, d in registros_por_fuente.items()
                          if campo in d and d[campo] == valor), None), 99
                )
                prioridad_fuente = FUENTE_PRIORIDADES.get(fuente, 99)

                if prioridad_fuente <= prioridad_actual:
                    consolidado[campo] = valor

    consolidado['fuentes_disponibles'] = ','.join(fuentes_disponibles)
    consolidado['fuente_principal'] = fuentes_ordenadas[0] if fuentes_ordenadas else None

    return consolidado


def resolve_paciente_key(registros_por_fuente: dict) -> tuple[str | None, datetime.date | None]:
    """
    Define la clave final del embarazo consolidado.

    Regla de negocio para esta primera ingesta:
    - Un solo embarazo final por DNI
    - La FPP que individualiza el embarazo se toma por prioridad de fuente:
      POF > WP > SUMAR > DERIVACIONES
    """
    if not registros_por_fuente:
        return None, None

    fuentes_ordenadas = sorted(
        registros_por_fuente.keys(),
        key=lambda f: FUENTE_PRIORIDADES.get(f, 99)
    )

    dni = None
    fpp = None

    for fuente in fuentes_ordenadas:
        datos = registros_por_fuente[fuente]
        if dni is None and datos.get("dni"):
            dni = str(datos["dni"])
        if fpp is None and datos.get("fecha_probable_parto"):
            fpp = datos["fecha_probable_parto"]
        if dni and fpp:
            break

    if fpp is None:
        for fuente in fuentes_ordenadas:
            datos = registros_por_fuente[fuente]
            fpp = _estimate_fpp_from_diagnosis(datos)
            if fpp:
                break

    return dni, fpp


def _estimate_fpp_from_diagnosis(datos_fuente: dict) -> datetime.date | None:
    """
    Estima FPP a 40 semanas cuando no viene informada.

    Reglas:
    - General: usar fecha_diagnostico_embarazo + edad gestacional
    - Derivaciones: si falta fecha_diagnostico_embarazo, usar fecha_derivacion
      (columna FECHA) + edad gestacional al momento de la derivacion
    """
    fecha_diagnostico = (
        datos_fuente.get("fecha_diagnostico_embarazo")
        or datos_fuente.get("fecha_derivacion")
    )
    edad_gestacional = datos_fuente.get("eg_actual") or datos_fuente.get("eg_segun_diagnostico")

    if not fecha_diagnostico or edad_gestacional is None:
        return None

    try:
        semanas = int(edad_gestacional)
    except (TypeError, ValueError):
        return None

    if semanas <= 0 or semanas > 45:
        return None

    semanas_restantes = max(40 - semanas, 0)
    return fecha_diagnostico + datetime.timedelta(weeks=semanas_restantes)


def _patient_payload(data: dict) -> dict:
    """Filtra metadatos auxiliares y deja solo columnas reales de pacientes_gold."""
    return {
        key: value
        for key, value in data.items()
        if key in PACIENTE_GOLD_COLUMNS
    }


def normalize_sisa_fields(data: dict) -> dict:
    """
    Normaliza códigos SISA antes de cualquier enriquecimiento derivado.
    """
    sisa_salud = _normalize_sisa_code(data.get("sisa_centro_salud"))
    if sisa_salud:
        data["sisa_centro_salud"] = sisa_salud

    sisa_derivado = _normalize_sisa_code(data.get("sisa_centro_derivado"))
    if sisa_derivado:
        data["sisa_centro_derivado"] = sisa_derivado

    return data


def enrich_with_efector_codes(session, data: dict) -> dict:
    """
    Normaliza establecimiento por código SISA oficial.

    Reglas:
    1) Si falta sisa_centro_salud, intentar derivarlo desde CUIE (SUMAR).
    2) Con sisa_centro_salud presente, normalizar SIEMPRE:
       - nombre_establecimiento <- efectores_sisa.nombre
       - localidad_establecimiento <- efectores_sisa.ciudad (fallback localidad)
    """
    sisa = data.get("sisa_centro_salud")

    # 1) SUMAR/CUIE -> SISA
    if not sisa:
        cuie_candidates = [
            data.get("cuie_seguimiento"),
            data.get("cuie_afiliacion"),
            data.get("prenatal_alta_cuie"),
            data.get("cuie_efector_prestacion"),
        ]
        cuie = next((c for c in cuie_candidates if c), None)
        if cuie:
            efector_by_cuie = session.query(Efector).filter_by(cuie=str(cuie)).first()
            if efector_by_cuie:
                codigo_sisa = getattr(efector_by_cuie, "codigo_sisa", None)
                if codigo_sisa:
                    sisa = _normalize_sisa_code(codigo_sisa)
                    data["sisa_centro_salud"] = sisa

    if not sisa:
        return data

    # 2) Normalización canónica por SISA (todas las fuentes)
    efector = session.query(Efector).filter_by(codigo_sisa=sisa).first()
    if not efector:
        return data

    if efector.nombre:
        data["nombre_establecimiento"] = efector.nombre
    ciudad = getattr(efector, "ciudad", None) or getattr(efector, "localidad", None)
    if ciudad:
        data["localidad_establecimiento"] = ciudad
    return data


def enrich_with_maternidad_id(session, data: dict) -> dict:
    """
    Puebla derivacion_maternidad_id buscando en maternidades el registro
    cuyo codigo_sisa coincida con sisa_centro_derivado del paciente.

    Solo aplica cuando hay derivacion (sisa_centro_derivado presente).
    Es la FK que habilita el filtrado RBAC para rol maternidad:
    estos usuarios ven los embarazos con sisa_centro_salud = su SISA
    MAS los que tienen derivacion_maternidad_id = su maternidad_id.
    """
    sisa_derivado = data.get("sisa_centro_derivado")
    if not sisa_derivado:
        return data

    maternidad = session.query(Maternidad).filter_by(codigo_sisa=str(sisa_derivado)).first()
    if not maternidad:
        logger.debug(f"  sisa_centro_derivado={sisa_derivado} sin match en tabla maternidades")
        return data

    data["derivacion_maternidad_id"] = maternidad.id
    logger.debug(f"  derivacion_maternidad_id={maternidad.id} ({maternidad.nombre}) desde sisa_derivado={sisa_derivado}")
    return data


def enrich_with_centro_derivado_name(session, data: dict) -> dict:
    """
    Normaliza nombre_centro_derivado a partir de sisa_centro_derivado.

    Prioridad de normalización:
    1) Tabla maternidades.codigo_sisa -> maternidades.nombre
    2) Tabla efectores_sisa.codigo_sisa -> efectores_sisa.nombre

    Esto asegura nombre canónico homogéneo entre todas las fuentes
    (vista SQL Server, Derivaciones y CAPS).
    """
    sisa_derivado = data.get("sisa_centro_derivado")
    if not sisa_derivado:
        return data

    maternidad = session.query(Maternidad).filter_by(codigo_sisa=sisa_derivado).first()
    if maternidad and maternidad.nombre:
        data["nombre_centro_derivado"] = maternidad.nombre
        return data

    efector = session.query(Efector).filter_by(codigo_sisa=sisa_derivado).first()
    if efector and efector.nombre:
        data["nombre_centro_derivado"] = efector.nombre

    return data


def backfill_centro_derivado_names(session) -> int:
    """
    Backfill de nombre_centro_derivado para todo el histórico en pacientes_gold.

    Se ejecuta al final de cada corrida para normalizar también registros
    antiguos que no entraron en el lote actual.
    """
    # Catálogo canónico de maternidades por SISA
    maternidades_map = {}
    for m in session.query(Maternidad).filter(Maternidad.codigo_sisa.isnot(None)).all():
        code = _normalize_sisa_code(m.codigo_sisa)
        if code and m.nombre:
            maternidades_map[code] = m.nombre

    # Catálogo alternativo de efectores por SISA
    efectores_map = {}
    for e in session.query(Efector).all():
        nombre = getattr(e, "nombre", None)
        if not nombre:
            continue
        code = _normalize_sisa_code(getattr(e, "codigo_sisa", None))
        if code and code not in efectores_map:
            efectores_map[code] = nombre

    actualizados = 0
    pacientes = session.query(PacienteGold).filter(PacienteGold.sisa_centro_derivado.isnot(None)).all()
    for p in pacientes:
        code = _normalize_sisa_code(p.sisa_centro_derivado)
        if not code:
            continue

        if p.sisa_centro_derivado != code:
            p.sisa_centro_derivado = code

        target = maternidades_map.get(code) or efectores_map.get(code)
        if target and p.nombre_centro_derivado != target:
            p.nombre_centro_derivado = target
            actualizados += 1

    return actualizados


# ─────────────────────────────────────────────────────────────────────────────
# UPSERT DE PACIENTES
# ─────────────────────────────────────────────────────────────────────────────

def upsert_paciente_gold(session, data: dict, batch_id: str):
    """
    Inserta o actualiza un paciente en pacientes_gold.

    Args:
        session: Sesión SQLAlchemy
        data: Diccionario con campos consolidados
        batch_id: ID del lote
    """
    dni = data.get('dni')
    fpp = data.get('fecha_probable_parto')
    eg_actual = data.get('eg_actual')
    fecha_derivacion = data.get('fecha_derivacion')
    fuente=data.get("fuente_principal") or data.get("fuente") or "desconocida"

    if not dni or not fpp:
        motivo = "sin_fpp"
        stage = PacienteSinFppStage(
            dni=str(dni) if dni else None,
            fuente=data.get("fuente_principal") or data.get("fuente") or "desconocida",
            motivo=motivo,
            batch_id=batch_id,
            data_json=json.dumps({k: str(v) for k, v in data.items() if v is not None}, default=str),
            ingestion_at=datetime.datetime.now(datetime.timezone.utc),
        )
        session.add(stage)
        logger.warning(f"  OMITIDO Registro sin clave completa: DNI={dni}, FPP={fpp}, EG={eg_actual}, FechaDerivacion={fecha_derivacion}")
        return None, False

    # Buscar existente por clave natural
    existing = session.query(PacienteGold).filter_by(
        dni=dni,
        fecha_probable_parto=fpp
    ).first()

    now = datetime.datetime.now(datetime.timezone.utc)

    # Campo de tracking según fuente principal
    fuente = data.get('fuente_principal', 'desconocida')
    track_field = f"ultima_actualizacion_{fuente}"
    patient_data = _patient_payload(data)

    if existing:
        # Actualizar campos no nulos
        for k, v in patient_data.items():
            if v is not None and k not in ['dni', 'fecha_probable_parto', 'id']:
                setattr(existing, k, v)

        # Actualizar tracking
        existing.batch_id = batch_id
        existing.ingestion_at = now
        if hasattr(existing, track_field):
            setattr(existing, track_field, now)

        existing.fuente = fuente
        paciente = existing
        is_new = False
        logger.debug(f"  ~ Actualizado: {dni}")

    else:
        # Insertar nuevo
        nuevo = PacienteGold(
            dni=dni,
            fecha_probable_parto=fpp,
            ingestion_at=now,
            batch_id=batch_id,
            fuente=fuente,
            **{k: v for k, v in patient_data.items() if k not in ['dni', 'fecha_probable_parto']}
        )
        session.add(nuevo)
        session.flush()
        paciente = nuevo
        is_new = True
        logger.debug(f"  + Insertado: {dni}")

    return paciente, is_new


def registrar_trazabilidad(session, paciente_id: int, fuente: str,
                           datos_crudos: dict, batch_id: str, source_info: dict):
    """Registra trazabilidad del dato en pacientes_gold_fuentes."""

    now = datetime.datetime.now(datetime.timezone.utc)
    data_json = json.dumps({k: str(v) for k, v in datos_crudos.items() if v is not None}, default=str)
    source_file = source_info.get('file', '')
    source_sheet = source_info.get('sheet', '')

    session.query(PacienteGoldFuentes).filter_by(
        paciente_id=paciente_id,
        fuente=fuente,
        source_file=source_file,
        source_sheet=source_sheet
    ).delete(synchronize_session=False)

    registro = PacienteGoldFuentes(
        paciente_id=paciente_id,
        fuente=fuente,
        batch_id=batch_id,
        data_json=data_json,
        source_file=source_file,
        source_sheet=source_sheet,
        ingestion_at=now
    )
    session.add(registro)


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_etl(fuentes: list = None, use_mock: bool = False):
    """
    Ejecuta el ETL para las fuentes especificadas.

    Args:
        fuentes: Lista de fuentes a procesar. Si None, procesa todas.
        use_mock: Si True, usa datos mock para testing
    """
    if fuentes is None:
        fuentes = ['v_embarazosdw', 'sumar', 'derivaciones', 'alto_riesgo_caps']

    print("\n" + "=" * 70)
    print("  ETL DE INGESTA - Sistema de Seguimiento Obstétrico")
    print("=" * 70)
    print(f"Fuentes a procesar: {', '.join(fuentes)}")
    print(f"Modo mock: {use_mock}")
    print(f"Prioridad: V_EmbarazosDW > SUMAR > Derivaciones")
    print("=" * 70 + "\n")

    session = Session()
    batch_id = str(uuid.uuid4())[:8]
    total_stats = {
        "insertados": 0,
        "actualizados": 0,
        "errores": 0,
        "sumar_con_cuie_sin_sisa": 0,
        "stage_sin_fnac": 0,
        "stage_eg_menor_2": 0,
    }

    # Diccionario para consolidar por paciente: {dni: {fuente: datos}}
    pacientes_pendientes = {}

    try:
        for fuente in fuentes:
            logger.info(f"Procesando fuente: {fuente.upper()}")

            # Cargar datos
            try:
                if fuente == "v_embarazosdw":
                    df = load_vista_source(use_mock=use_mock)
                    persist_vista_raw(session, df, batch_id)
                    df = dedupe_vista(df)
                elif use_mock:
                    config = SOURCES_CONFIG.get(fuente, {})
                    loader = MockGoogleSheetsLoader()
                    sheet_names = config.get("sheet_names") or [config.get("sheet_name", fuente)]
                    df = loader.load_multiple_sheets(config.get("sheet_id", "mock"), sheet_names)
                else:
                    df = load_google_source(fuente)
            except Exception as e:
                logger.error(f"  ERROR cargando {fuente.upper()}: {e}")
                total_stats["errores"] += 1
                continue

            if df.empty:
                logger.warning(f"  OMITIDO {fuente.upper()}: DataFrame vacio")
                continue

            logger.info(f"  {len(df)} filas leidas")

            # Procesar filas y agrupar por paciente
            config = SOURCES_CONFIG.get(fuente, {})
            source_info = {
                "file": "V_EmbarazosDW" if fuente == "v_embarazosdw" else config.get("source_name", f"google_sheets_{fuente}"),
                "sheet": "V_EmbarazosDW" if fuente == "v_embarazosdw" else config.get("sheet_name", ""),
            }

            for idx, row in df.iterrows():
                try:
                    data = process_row(row, fuente)
                    data["_source_sheet"] = row.get("_source_sheet") or config.get("sheet_name", "")
                    
                    dni = data.get('dni')
                    if not dni:
                        logger.debug(f"  Fila {idx} sin DNI: enviando a stage...")
                        # Enviar fila a tabla stage para DNI nulos o huérfanos
                        stage_record = PacienteSinDniStage(
                            fuente=fuente,
                            hoja=data["_source_sheet"][:100],
                            batch_id=batch_id,
                            data_json=json.dumps({k: str(v) for k, v in data.items() if v is not None}, default=str),
                            ingestion_at=datetime.datetime.now(datetime.timezone.utc)
                        )
                        session.add(stage_record)
                        continue

                    if data.get("_stage_motivo"):
                        stage_record = PacienteSinFppStage(
                            dni=str(dni),
                            fuente=fuente,
                            motivo=data["_stage_motivo"],
                            batch_id=batch_id,
                            data_json=json.dumps({k: str(v) for k, v in data.items() if v is not None}, default=str),
                            ingestion_at=datetime.datetime.now(datetime.timezone.utc)
                        )
                        session.add(stage_record)
                        continue

                    key = str(dni)
                    if key not in pacientes_pendientes:
                        pacientes_pendientes[key] = {}

                    pacientes_pendientes[key][fuente] = data

                except Exception as e:
                    logger.warning(f"  ERROR fila {idx}: {e}")
                    total_stats["errores"] += 1

            session.commit() # Save stage records
            logger.info(f"  OK {fuente.upper()} procesada")

        # Consolidar y guardar pacientes
        logger.info(f"\nConsolidando {len(pacientes_pendientes)} pacientes unicos...")

        for idx, (dni_key, registros_por_fuente) in enumerate(pacientes_pendientes.items()):
            try:
                # Consolidar según prioridades
                data_consolidado = consolidate_patient_data(registros_por_fuente)
                dni_final, fpp_final = resolve_paciente_key(registros_por_fuente)
                data_consolidado["dni"] = dni_final
                data_consolidado["fecha_probable_parto"] = fpp_final
                data_consolidado = enforce_embarazo_en_curso_rules(data_consolidado)
                # EG canónica desde FPP consolidada solo para embarazos en curso.
                if _should_recalculate_eg(data_consolidado):
                    data_consolidado["eg_actual"] = _calculate_eg_actual_from_fpp(fpp_final)
                data_consolidado = normalize_sisa_fields(data_consolidado)
                data_consolidado = enrich_with_efector_codes(session, data_consolidado)
                data_consolidado = enrich_with_centro_derivado_name(session, data_consolidado)
                data_consolidado = enrich_with_maternidad_id(session, data_consolidado)

                # Recalcular edad_actual para todas las fuentes desde fecha_nacimiento.
                fecha_nac = data_consolidado.get("fecha_nacimiento")
                edad_calc = _calculate_age_years(fecha_nac)

                if fecha_nac is None:
                    _stage_fnac_issue(session, data_consolidado, batch_id, "sin_fecha_nacimiento", None)
                    total_stats["stage_sin_fnac"] += 1
                    continue

                data_consolidado["edad_actual"] = edad_calc
                if edad_calc is None or edad_calc < 10:
                    _stage_fnac_issue(session, data_consolidado, batch_id, "edad_menor_10", edad_calc)
                    total_stats["stage_sin_fnac"] += 1
                    continue

                # Regla de riesgo por edad gestante adolescente.
                if edad_calc < 15:
                    data_consolidado["riesgo"] = "SI"

                eg_val = data_consolidado.get("eg_actual")
                try:
                    eg_float = float(eg_val) if eg_val is not None else None
                except (TypeError, ValueError):
                    eg_float = None
                if eg_float is not None and eg_float < 2:
                    _stage_eg_issue(session, data_consolidado, batch_id, eg_float)
                    total_stats["stage_eg_menor_2"] += 1
                    continue

                data_consolidado = normalize_uppercase_fields(data_consolidado)

                if (
                    "sumar" in registros_por_fuente
                    and data_consolidado.get("cuie_seguimiento")
                    and not data_consolidado.get("sisa_centro_salud")
                ):
                    total_stats["sumar_con_cuie_sin_sisa"] += 1

                # Guardar en pacientes_gold
                paciente, is_new = upsert_paciente_gold(session, data_consolidado, batch_id)

                if paciente:
                    # Registrar trazabilidad por cada fuente
                    for fuente, datos_crudos in registros_por_fuente.items():
                        source_config = SOURCES_CONFIG.get(fuente, {})
                        registrar_trazabilidad(
                            session, paciente.id, fuente, datos_crudos,
                            batch_id,
                            {
                                "file": "V_EmbarazosDW" if fuente == "v_embarazosdw" else source_config.get("source_name", f"google_sheets_{fuente}"),
                                "sheet": "V_EmbarazosDW" if fuente == "v_embarazosdw" else datos_crudos.get("_source_sheet", source_config.get("sheet_name", "")),
                            }
                        )

                    if idx % 100 == 0:
                        session.commit()
                        logger.info(f"    ... {idx + 1} pacientes procesados")

                    total_stats["insertados" if is_new else "actualizados"] += 1

            except Exception as e:
                logger.warning(f"  ERROR consolidando {dni_key}: {e}")
                total_stats["errores"] += 1
                session.rollback()

        # Backfill histórico: normalizar nombre_centro_derivado por SISA
        total_stats["nombre_centro_derivado_backfill"] = backfill_centro_derivado_names(session)
        session.commit()

        # Resumen final
        print("\n" + "=" * 70)
        print(f"  RESULTADO FINAL")
        print("=" * 70)
        print(f"  + {total_stats['insertados']} pacientes insertados")
        print(f"  ~ {total_stats['actualizados']} pacientes actualizados")
        print(f"  - {total_stats['errores']} errores")
        print(f"  ! {total_stats['sumar_con_cuie_sin_sisa']} casos SUMAR con CUIE sin Codigo SISA resuelto")
        print(f"  ! {total_stats['stage_sin_fnac']} registros enviados a stage por FNac/edad")
        print(f"  ! {total_stats['stage_eg_menor_2']} registros enviados a stage por EG<2 semanas")
        print(f"  ! {total_stats.get('nombre_centro_derivado_backfill', 0)} nombres de centro derivado normalizados (histórico)")
        print(f"  Batch ID: {batch_id}")
        print("=" * 70 + "\n")

    except Exception as e:
        session.rollback()
        logger.error(f"ERROR GENERAL: {e}")
        raise

    finally:
        session.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETL de ingesta de fuentes")
    parser.add_argument(
        "--fuentes", "-f",
        nargs="+",
        choices=['v_embarazosdw', 'pof', 'wp', 'derivaciones', 'sumar', 'alto_riesgo_caps'],
        help="Fuentes a procesar"
    )
    parser.add_argument(
        "--mock", "-m",
        action="store_true",
        help="Usar loaders mock (sin conexión real)"
    )

    args = parser.parse_args()
    run_etl(fuentes=args.fuentes, use_mock=args.mock)

"""
Módulo para cargar datos desde Google Sheets.

Responsabilidades:
- Conectar a Google Sheets API usando service account
- Leer sheets por ID y nombre de hoja
- Retornar DataFrames pandas para procesamiento ETL

Uso:
    from etl.google_sheets_loader import GoogleSheetsLoader, SOURCES_CONFIG

    loader = GoogleSheetsLoader("seguimiento-embarazo-c9a747ef8653.json")
    df = loader.load_sheet("SHEET_ID_AQUI", "embarazadas")
"""

import os
import logging
import time
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

logger = logging.getLogger(__name__)

# Scopes necesarios para solo lectura de Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
RETRYABLE_STATUS_CODES = ("[429]", "[500]", "[502]", "[503]", "[504]")


def _env_or_default(env_name: str, default: str) -> str:
    value = os.getenv(env_name)
    return value.strip() if value else default


class GoogleSheetsLoader:
    """Loader para Google Sheets con soporte para múltiples credenciales."""

    def __init__(self, credentials_path: str):
        """
        Inicializa el loader con credenciales de service account.

        Args:
            credentials_path: Ruta al archivo JSON de credenciales
        """
        self.credentials_path = credentials_path
        self._client = None

    def _get_client(self) -> gspread.Client:
        """Obtiene o crea el cliente de gspread (lazy initialization)."""
        if self._client is None:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credenciales no encontradas: {self.credentials_path}")

            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=SCOPES
            )
            self._client = gspread.authorize(creds)
            logger.info(f"Conectado a Google Sheets API")
        return self._client

    def _is_retryable_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        if any(code.lower() in text for code in RETRYABLE_STATUS_CODES):
            return True
        return "timeout" in text or "temporarily unavailable" in text

    def _run_with_retries(self, fn, description: str, max_attempts: int = 4):
        """
        Ejecuta una operación de Google API con reintentos y backoff.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as exc:
                is_retryable = self._is_retryable_error(exc)
                is_last = attempt == max_attempts
                if not is_retryable or is_last:
                    raise
                wait_seconds = 2 ** (attempt - 1)
                logger.warning(
                    f"{description} falló (intento {attempt}/{max_attempts}): {exc}. "
                    f"Reintentando en {wait_seconds}s..."
                )
                time.sleep(wait_seconds)

    def load_sheet(
        self,
        sheet_id: str,
        sheet_name: str = None,
        header_row: int = 1,
        start_col: int = 0,
    ) -> pd.DataFrame:
        """
        Carga una hoja desde Google Sheets con soporte para encabezados desplazados.

        Args:
            sheet_id: ID del spreadsheet (de la URL o compartir)
            sheet_name: Nombre de la hoja. Si es None, usa la primera hoja.
            header_row: Fila donde está el encabezado (1-based, default 1).
            start_col: Columna de inicio en índice 0-based (default 0 = columna A).
                       Usar 1 para empezar desde la columna B.

        Returns:
            DataFrame con los datos de la hoja
        """
        client = self._get_client()

        try:
            spreadsheet = self._run_with_retries(
                lambda: client.open_by_key(sheet_id),
                f"Apertura de spreadsheet {sheet_id}",
            )
            logger.info(f"Abriendo sheet: {spreadsheet.title} (ID: {sheet_id})")

            if sheet_name:
                try:
                    worksheet = self._run_with_retries(
                        lambda: spreadsheet.worksheet(sheet_name),
                        f"Acceso a worksheet {sheet_name}",
                    )
                except gspread.exceptions.WorksheetNotFound:
                    available = [ws.title for ws in spreadsheet.worksheets()]
                    raise ValueError(
                        f"Sheet '{sheet_name}' no encontrado. Disponibles: {available}"
                    )
            else:
                worksheet = spreadsheet.sheet1
                sheet_name = worksheet.title

            # Leer todos los valores crudos para controlar posición de encabezado
            filas = self._run_with_retries(
                lambda: worksheet.get_all_values(),
                f"Lectura de valores {sheet_name}",
            )

            if not filas or len(filas) < header_row:
                logger.warning(f"  → Sin datos en '{sheet_name}'")
                return pd.DataFrame()

            header_idx = header_row - 1  # convertir a 0-based
            encabezados_raw = filas[header_idx][start_col:]

            # Renombrar columnas vacías o duplicadas
            cols_vistas = {}
            encabezados = []
            for i, col in enumerate(encabezados_raw):
                col = col.strip() if col else f"_col_{i + start_col}"
                if col in cols_vistas:
                    cols_vistas[col] += 1
                    col = f"{col}_{cols_vistas[col]}"
                else:
                    cols_vistas[col] = 0
                encabezados.append(col)

            datos = [fila[start_col:] for fila in filas[header_row:]]
            df = pd.DataFrame(datos, columns=encabezados)

            # Eliminar filas completamente vacías
            df = df.replace("", pd.NA).dropna(how="all").reset_index(drop=True)

            logger.info(f"  → {len(df)} filas cargadas desde '{sheet_name}'")
            logger.info(f"  → Columnas: {list(df.columns)[:10]}...")

            return df

        except gspread.exceptions.SpreadsheetNotFound:
            raise ValueError(
                f"Spreadsheet ID '{sheet_id}' no encontrado. "
                f"Verificar que el service account tenga acceso de lectura."
            )
        except Exception as e:
            logger.error(f"Error cargando sheet {sheet_id}/{sheet_name}: {e}")
            raise

    def load_multiple_sheets(
        self,
        sheet_id: str,
        sheet_names: list[str],
        header_rows: dict = None,
        start_col: int = 0,
    ) -> pd.DataFrame:
        """
        Carga múltiples hojas de un mismo spreadsheet y concatena sus filas.

        Args:
            sheet_id: ID del spreadsheet
            sheet_names: Nombres de hojas a procesar
            header_rows: Dict opcional {nombre_hoja: fila_encabezado (1-based)}.
                         Si una hoja no está en el dict, se usa fila 1.
            start_col: Columna de inicio 0-based compartida para todas las hojas (default 0).

        Returns:
            DataFrame concatenado con columna auxiliar '_source_sheet'
        """
        frames = []

        for sheet_name in sheet_names:
            header_row = (header_rows or {}).get(sheet_name, 1)
            df = self.load_sheet(sheet_id, sheet_name, header_row=header_row, start_col=start_col)
            if df.empty:
                continue
            df["_source_sheet"] = sheet_name
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def list_sheets(self, sheet_id: str) -> list:
        """
        Lista todas las hojas disponibles en un spreadsheet.

        Args:
            sheet_id: ID del spreadsheet

        Returns:
            Lista de nombres de hojas
        """
        client = self._get_client()
        spreadsheet = client.open_by_key(sheet_id)
        return [ws.title for ws in spreadsheet.worksheets()]


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE FUENTES DESDE GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
#
# IMPORTANTE: Reemplazar los SHEET_ID con los IDs reales de tus Google Sheets
# Los IDs se encuentran en la URL:
# https://docs.google.com/spreadsheets/d/ESTE_ES_EL_ID/edit#gid=0
#
# Las credenciales (JSON) están en la raíz del proyecto:
# - seguimiento-embarazo-c9a747ef8653.json
# - seguimiento-embarazo-f7855785468a.json
# ─────────────────────────────────────────────────────────────────────────────

SOURCES_CONFIG = {
    "pof": {
        "source_kind": "drive_excel",
        "sheet_id": os.getenv("GSHEET_POF_ID", "TU_SHEET_ID_AQUI"),
        "sheet_name": "embarazadas",
        "sheet_names": ["embarazadas"],
        "source_name": "20260410_EmbarazosEnCurso_POF.xlsx",
        "credentials": _env_or_default(
            "GSHEET_POF_CREDENTIALS",
            "seguimiento-embarazo-c9a747ef8653.json"
        ),
    },
    "wp": {
        "source_kind": "drive_excel",
        "sheet_id": os.getenv("GSHEET_WP_ID", "TU_SHEET_ID_AQUI"),
        "sheet_name": "embarazadas",
        "sheet_names": ["embarazadas"],
        "source_name": "WPListadoGeneralEmbarazos.xlsx",
        "credentials": _env_or_default(
            "GSHEET_WP_CREDENTIALS",
            "seguimiento-embarazo-c9a747ef8653.json"
        ),
    },
    "derivaciones": {
        "source_kind": "drive_excel",
        "sheet_id": os.getenv("GSHEET_DERIVACIONES_ID", "TU_SHEET_ID_AQUI"),
        "sheet_name": "ENERO/FEBRERO/MARZO/ABRIL/MAYO",
        "sheet_names": ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO"],
        "source_name": "DERIVACIONES 2026 RED obstetrica AR (1) (1).xlsx",
        "credentials": _env_or_default(
            "GSHEET_DERIVACIONES_CREDENTIALS",
            "seguimiento-embarazo-c9a747ef8653.json"
        ),
    },
    "sumar": {
        "source_kind": "drive_excel",
        "sheet_id": os.getenv("GSHEET_SUMAR_ID", "TU_SHEET_ID_AQUI"),
        "sheet_name": "Embarazadas",
        "sheet_names": ["Embarazadas"],
        "source_name": "EmbarazadasSUMAR.xlsx",
        "credentials": _env_or_default(
            "GSHEET_SUMAR_CREDENTIALS",
            "seguimiento-embarazo-f7855785468a.json"
        ),
    },
    "alto_riesgo_caps": {
        "source_kind": "google_sheets",
        "sheet_id": os.getenv("GSHEET_ALTO_RIESGO_CAPS_ID", "1pVSGXUN0XPXLsY3ijt6FjnMNwTD_6sqyBrF8G-Zd2sc"),
        "sheet_name": "HOSPITAL VIDAL / HOSPITAL LLANO",
        "sheet_names": ["HOSPITAL VIDAL", "HOSPITAL LLANO"],
        "source_name": "EMBARAZADAS DERIVADAS ALTO RIESGO CAPS",
        "credentials": _env_or_default(
            "GSHEET_ALTO_RIESGO_CAPS_CREDENTIALS",
            "seguimiento-embarazo-c9a747ef8653.json"
        ),
        # Fila donde está el encabezado en cada hoja (1-based, como en Google Sheets)
        "header_rows": {
            "HOSPITAL VIDAL": 3,   # encabezado en fila 3
            "HOSPITAL LLANO": 4,   # encabezado en fila 4
        },
        "start_col": 1,  # columna B (índice 1, 0-based) es la primera columna de datos
    },
}


def get_available_sheets(credentials_file: str = None) -> dict:
    """
    Lista todos los sheets accesibles con las credenciales dadas.

    Args:
        credentials_file: Nombre del archivo de credenciales (en la raíz del proyecto)

    Returns:
        Dict con sheet_id -> lista de hojas
    """
    if credentials_file is None:
        credentials_file = "seguimiento-embarazo-c9a747ef8653.json"

    credentials_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        credentials_file
    )

    loader = GoogleSheetsLoader(credentials_path)

    # Nota: Necesitas conocer los sheet_ids previamente
    # Esta función es útil para verificar acceso una vez que tienes los IDs
    return {
        "pof": SOURCES_CONFIG["pof"]["sheet_name"],
        "wp": SOURCES_CONFIG["wp"]["sheet_name"],
        "derivaciones": SOURCES_CONFIG["derivaciones"]["sheet_name"],
        "sumar": SOURCES_CONFIG["sumar"]["sheet_name"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# MOCK PARA TESTING
# ─────────────────────────────────────────────────────────────────────────────

class MockGoogleSheetsLoader:
    """
    Loader mock para testing sin credenciales reales.
    Retorna DataFrames vacíos con la estructura esperada.
    """

    def load_sheet(self, sheet_id: str, sheet_name: str) -> pd.DataFrame:
        """Retorna DataFrame vacío con columnas mock."""
        logger.warning(f"Usando MOCK loader para sheet {sheet_id}/{sheet_name}")

        # Columnas base esperadas (según mapeo)
        mock_cols = [
            "dni", "apellido", "nombre", "fecha_probable_parto",
            "fecha_nacimiento", "edad_actual", "riesgo",
            "fecha_ultimo_control", "proxima_cita"
        ]
        return pd.DataFrame(columns=mock_cols)

    def list_sheets(self, sheet_id: str) -> list:
        return ["embarazadas", "historico"]

    def load_multiple_sheets(self, sheet_id: str, sheet_names: list[str]) -> pd.DataFrame:
        logger.warning(f"Usando MOCK loader para multiples hojas {sheet_names} del sheet {sheet_id}")
        return self.load_sheet(sheet_id, sheet_names[0] if sheet_names else "mock")

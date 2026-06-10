"""
Loader para documentos alojados en Google Drive.

Soporta dos escenarios:
- Archivos Excel almacenados en Drive
- Spreadsheets nativos de Google exportados temporalmente a XLSX
"""

import io
import logging
import os

import pandas as pd
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

logger = logging.getLogger(__name__)

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

GOOGLE_SHEETS_MIME = "application/vnd.google-apps.spreadsheet"


class GoogleDriveExcelLoader:
    """Descarga archivos desde Drive y los procesa como Excel."""

    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self._session = None

    def _get_session(self) -> AuthorizedSession:
        if self._session is None:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credenciales no encontradas: {self.credentials_path}")

            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=DRIVE_SCOPES,
            )
            self._session = AuthorizedSession(creds)
            logger.info("Conectado a Google Drive API")

        return self._session

    def get_file_metadata(self, file_id: str) -> dict:
        session = self._get_session()
        response = session.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            params={"fields": "id,name,mimeType"},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _download_file_bytes(self, file_id: str) -> bytes:
        session = self._get_session()
        metadata = self.get_file_metadata(file_id)
        mime_type = metadata.get("mimeType")

        if mime_type == GOOGLE_SHEETS_MIME:
            response = session.get(
                f"https://www.googleapis.com/drive/v3/files/{file_id}/export",
                params={
                    "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                },
                timeout=60,
            )
        else:
            response = session.get(
                f"https://www.googleapis.com/drive/v3/files/{file_id}",
                params={"alt": "media"},
                timeout=60,
            )

        response.raise_for_status()
        return response.content

    def list_sheets(self, file_id: str) -> list[str]:
        content = self._download_file_bytes(file_id)
        excel = pd.ExcelFile(io.BytesIO(content))
        return excel.sheet_names

    @staticmethod
    def _resolve_sheet_name(excel: pd.ExcelFile, requested_sheet_name: str) -> str:
        normalized_requested = requested_sheet_name.strip().casefold()
        for available_sheet in excel.sheet_names:
            if available_sheet.strip().casefold() == normalized_requested:
                return available_sheet
        raise ValueError(
            f"Sheet '{requested_sheet_name}' no encontrado. Disponibles: {excel.sheet_names}"
        )

    def load_sheet(
        self,
        file_id: str,
        sheet_name: str,
        header_row: int = 1,
        start_col: int = 0,
    ) -> pd.DataFrame:
        """
        Carga una hoja desde un archivo en Drive con soporte para encabezados desplazados.

        Args:
            file_id: ID del archivo en Google Drive
            sheet_name: Nombre de la hoja a cargar
            header_row: Fila donde está el encabezado (1-based, default 1)
            start_col: Columna de inicio en índice 0-based (default 0 = columna A)

        Returns:
            DataFrame con los datos de la hoja
        """
        content = self._download_file_bytes(file_id)
        excel = pd.ExcelFile(io.BytesIO(content))
        resolved_sheet = self._resolve_sheet_name(excel, sheet_name)

        df = pd.read_excel(
            excel,
            sheet_name=resolved_sheet,
            header=header_row - 1,  # convertir a 0-based
            dtype=str,
        )

        if start_col > 0:
            df = df.iloc[:, start_col:]

        df = df.replace("", pd.NA).dropna(how="all").reset_index(drop=True)

        logger.info(f"  -> {len(df)} filas cargadas desde '{sheet_name}'")
        logger.info(f"  -> Columnas: {list(df.columns)[:10]}...")
        return df

    def load_multiple_sheets(
        self,
        file_id: str,
        sheet_names: list[str],
        header_rows: dict = None,
        start_col: int = 0,
    ) -> pd.DataFrame:
        """
        Carga múltiples hojas de un archivo en Drive y concatena sus filas.

        Args:
            file_id: ID del archivo en Google Drive
            sheet_names: Nombres de hojas a procesar
            header_rows: Dict opcional {nombre_hoja: fila_encabezado (1-based)}.
                         Si una hoja no está en el dict, se usa fila 1.
            start_col: Columna de inicio 0-based compartida para todas las hojas (default 0)

        Returns:
            DataFrame concatenado con columna auxiliar '_source_sheet'
        """
        content = self._download_file_bytes(file_id)
        excel = pd.ExcelFile(io.BytesIO(content))
        frames = []

        for sheet_name in sheet_names:
            resolved_sheet = self._resolve_sheet_name(excel, sheet_name)

            # header_row en pandas es 0-based, en nuestra config es 1-based
            header_row_1based = (header_rows or {}).get(sheet_name, 1)
            header_row_0based = header_row_1based - 1

            df = pd.read_excel(
                excel,
                sheet_name=resolved_sheet,
                header=header_row_0based,
                dtype=str,
            )

            if start_col > 0:
                df = df.iloc[:, start_col:]

            # Eliminar filas completamente vacías
            df = df.replace("", pd.NA).dropna(how="all").reset_index(drop=True)

            if df.empty:
                continue

            df["_source_sheet"] = resolved_sheet
            frames.append(df)
            logger.info(f"  -> {len(df)} filas cargadas desde '{sheet_name}'")
            logger.info(f"  -> Columnas: {list(df.columns)[:10]}...")

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

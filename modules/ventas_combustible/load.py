"""
Loader para Ventas de Combustible
Carga datos en base de datos y actualiza Google Sheets
"""
from typing import Any, Dict, Optional
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from json import loads
import os
from core.base_loader import BaseLoader
from utils.db import DatabaseConnection
from config.settings import Settings


class VentasCombustibleLoader(BaseLoader):
    """Loader para datos de ventas de combustible"""
    
    def __init__(self, db_config: Optional[Dict[str, str]] = None):
        super().__init__("ventas_combustible", db_config)
        self.settings = Settings()
        self.db_connection: Optional[DatabaseConnection] = None
        self.table_name = 'combustible'
        self.date_column = 'fecha'
    
    def load(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        Carga los datos en la base de datos y actualiza Google Sheets si hay nuevos datos.
        
        Args:
            data: DataFrame transformado a cargar
            suma_mensual: Suma mensual para actualizar Google Sheets (opcional)
            
        Returns:
            True si se cargaron nuevos datos, False en caso contrario
        """
        suma_mensual = kwargs.get('suma_mensual')
        
        # Conectar a BD
        self.db_connection = DatabaseConnection(
            database_type='economico'
        )
        self.db_connection.connect()
        
        try:
            # Obtener última fecha en BD
            ultima_fecha_bd = self.db_connection.get_max_date(
                self.table_name,
                self.date_column
            )
            
            self.logger.info(f"Última fecha en BD: {ultima_fecha_bd}")
            
            # Filtrar datos nuevos
            if ultima_fecha_bd:
                data['fecha'] = pd.to_datetime(data['fecha'])
                df_nuevos = data[data['fecha'] > ultima_fecha_bd]
            else:
                df_nuevos = data
                self.logger.info("No hay datos previos, cargando todos los datos")
            
            if df_nuevos.empty:
                self.logger.info("No hay nuevos datos para cargar")
                return False
            
            self.logger.info(f"Cargando {len(df_nuevos)} nuevos registros")
            
            # Determinar si la tabla existe
            table_exists = self.db_connection.table_exists(self.table_name)
            if_exists = 'append' if table_exists else 'replace'
            
            # Cargar datos
            self.db_connection.load_dataframe(
                df_nuevos,
                self.table_name,
                if_exists=if_exists
            )
            
            self.db_connection.commit()
            self.logger.info("Datos cargados exitosamente en BD")
            
            # Actualizar Google Sheets si se proporciona suma_mensual
            if suma_mensual is not None:
                self._update_google_sheets(suma_mensual)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error durante la carga: {e}")
            self.db_connection.rollback()
            raise
    
    def _update_google_sheets(self, suma_mensual: float) -> None:
        """Actualiza Google Sheets con la suma mensual"""
        try:
            # Configuración de Google Sheets
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            key_dict = loads(self.settings.GOOGLE_SHEETS_API_KEY)
            SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'
            
            # Autenticar
            creds = service_account.Credentials.from_service_account_info(
                key_dict,
                scopes=SCOPES
            )
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            
            # Leer fila 6 para encontrar última columna
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range='Datos!6:6'
            ).execute()
            
            values = result.get('values', [])
            last_column = len(values[0]) if values and values[0] else 0
            
            # Convertir índice a letra de columna
            column_letter = self._num_to_col(last_column)
            range_to_update = f'Datos!{column_letter}6'
            
            # Actualizar celda
            request = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_to_update,
                valueInputOption='RAW',
                body={'values': [[suma_mensual]]}
            ).execute()
            
            if request.get('updatedCells'):
                self.logger.info(f"Google Sheets actualizado: {request.get('updatedCells')} celdas")
            else:
                self.logger.warning("No se actualizaron celdas en Google Sheets")
                
        except Exception as e:
            self.logger.error(f"Error al actualizar Google Sheets: {e}")
            # No lanzar excepción, solo loggear
    
    def _num_to_col(self, n: int) -> str:
        """Convierte número de columna a letra (A, B, C, ..., Z, AA, AB, ...)"""
        result = ""
        while n >= 0:
            result = chr(n % 26 + 65) + result
            n = n // 26 - 1
        return result
    
    def close_connections(self) -> None:
        """Cierra conexiones"""
        if self.db_connection:
            self.db_connection.close()
        super().close_connections()




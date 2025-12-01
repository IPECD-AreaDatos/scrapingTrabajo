"""
Utilidades para manejo de fechas
"""
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd


class DateHelper:
    """Clase helper para operaciones con fechas"""
    
    @staticmethod
    def now() -> datetime:
        """Retorna la fecha/hora actual"""
        return datetime.now()
    
    @staticmethod
    def today() -> datetime:
        """Retorna la fecha de hoy a medianoche"""
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def format_date(date: datetime, format_str: str = '%Y-%m-%d') -> str:
        """
        Formatea una fecha según el formato especificado.
        
        Args:
            date: Fecha a formatear
            format_str: Formato deseado
            
        Returns:
            Fecha formateada como string
        """
        return date.strftime(format_str)
    
    @staticmethod
    def parse_date(date_str: str, format_str: Optional[str] = None) -> datetime:
        """
        Parsea un string a datetime.
        
        Args:
            date_str: String con la fecha
            format_str: Formato esperado (opcional, intenta inferir si no se proporciona)
            
        Returns:
            Objeto datetime
        """
        if format_str:
            return datetime.strptime(date_str, format_str)
        return pd.to_datetime(date_str)
    
    @staticmethod
    def get_last_month() -> tuple[datetime, datetime]:
        """
        Obtiene el primer y último día del mes anterior.
        
        Returns:
            Tupla (primer_dia, ultimo_dia) del mes anterior
        """
        today = datetime.now()
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - timedelta(days=1)
        first_day_previous = last_day_previous.replace(day=1)
        
        return first_day_previous, last_day_previous
    
    @staticmethod
    def get_month_range(year: int, month: int) -> tuple[datetime, datetime]:
        """
        Obtiene el primer y último día de un mes específico.
        
        Args:
            year: Año
            month: Mes (1-12)
            
        Returns:
            Tupla (primer_dia, ultimo_dia) del mes
        """
        first_day = datetime(year, month, 1)
        
        if month == 12:
            last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        return first_day, last_day
    
    @staticmethod
    def days_between(date1: datetime, date2: datetime) -> int:
        """
        Calcula la diferencia en días entre dos fechas.
        
        Args:
            date1: Primera fecha
            date2: Segunda fecha
            
        Returns:
            Diferencia en días
        """
        return abs((date2 - date1).days)




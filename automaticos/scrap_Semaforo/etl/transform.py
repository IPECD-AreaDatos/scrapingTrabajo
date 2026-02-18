"""
TRANSFORM - Módulo de transformación de datos Semáforo
Responsabilidad: Limpiar y formatear los DataFrames extraídos de Sheets
"""
import logging
import datetime
import pandas as pd

logger = logging.getLogger(__name__)

MESES = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
}


class TransformSemaforo:
    """Transforma los DataFrames crudos del Semáforo."""

    def transform(self, df_interanual: pd.DataFrame, df_intermensual: pd.DataFrame):
        """
        Aplica transformaciones a ambos DataFrames.

        Returns:
            tuple: (df_interanual_transformado, df_intermensual_transformado)
        """
        logger.info("[TRANSFORM] Transformando datos interanuales...")
        df_inter = self._transformar(df_interanual)
        logger.info("[TRANSFORM] Transformando datos intermensuales...")
        df_interm = self._transformar(df_intermensual)
        return df_inter, df_interm

    def _transformar(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica limpieza de porcentajes y conversión de fechas."""
        df = df.copy()
        df['fecha'] = df['fecha'].apply(self._convertir_fecha)

        for col in df.columns:
            if col == 'fecha':
                continue
            if df[col].dtype == 'object':
                try:
                    df[col] = (
                        df[col]
                        .str.replace('%', '', regex=False)
                        .str.replace(',', '.', regex=False)
                    )
                    df[col] = pd.to_numeric(df[col], errors='coerce') / 100
                    df[col] = df[col].apply(
                        lambda x: self._truncar_float(x, 3) if pd.notnull(x) else x
                    )
                except Exception as e:
                    logger.warning("[TRANSFORM] Error procesando columna '%s': %s", col, e)

        return df

    @staticmethod
    def _convertir_fecha(valor: str) -> datetime.datetime:
        """Convierte 'mes-aa' a datetime (ej: 'ene-23' → 2023-01-01)."""
        partes = valor.split('-')
        mes_nombre = partes[0].lower()
        anio = int(partes[1]) + 2000
        mes = MESES.get(mes_nombre)
        if mes is None:
            raise ValueError(f"Nombre de mes inválido: '{mes_nombre}'")
        return datetime.datetime(anio, mes, 1)

    @staticmethod
    def _truncar_float(valor: float, decimales: int) -> float:
        """Trunca un float a N decimales (sin redondear)."""
        s = str(valor)
        punto = s.find('.')
        if punto == -1:
            return float(valor)
        return float(s[:punto + decimales + 1])

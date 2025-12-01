"""
Transformer para Ventas de Combustible
Transforma el CSV descargado en DataFrame listo para cargar
"""
from typing import Any
from pathlib import Path
import pandas as pd
from core.base_transformer import BaseTransformer


class VentasCombustibleTransformer(BaseTransformer):
    """Transformer para datos de ventas de combustible"""
    
    def __init__(self):
        super().__init__("ventas_combustible")
        
        # Diccionario de provincias
        self.dict_provincias = {
            'Estado Nacional': 1,
            'Capital Federal': 2,
            'Buenos Aires': 6,
            'Catamarca': 10,
            'Chaco': 22,
            'Chubut': 26,
            'Córdoba': 14,
            'Corrientes': 18,
            'Entre Rios': 30,
            'Formosa': 34,
            'Jujuy': 38,
            'La Pampa': 42,
            'La Rioja': 46,
            'Mendoza': 50,
            'Misiones': 54,
            'Neuquén': 58,
            'Rio Negro': 62,
            'Salta': 66,
            'San Juan': 70,
            'San Luis': 74,
            'Santa Cruz': 78,
            'Santa Fe': 82,
            'Santiago del Estero': 86,
            'Tierra del Fuego': 94,
            'Tucuman': 90,
        }
    
    def transform(self, data: dict, **kwargs) -> pd.DataFrame:
        """
        Transforma el archivo CSV en DataFrame procesado.
        
        Args:
            data: Diccionario con 'file_path' del archivo CSV
            
        Returns:
            DataFrame transformado con columnas: fecha, producto, provincia, cantidad
        """
        file_path = Path(data['file_path'])
        
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        # Leer CSV
        self.logger.info(f"Leyendo archivo: {file_path}")
        df = pd.read_csv(file_path)
        
        # Aplicar transformaciones
        df = self._transformar_columnas(df)
        df = self._transformar_provincia(df)
        df = df.drop(columns=['unidad'], errors='ignore')
        
        self.logger.info(f"Transformación completada: {len(df)} filas")
        return df
    
    def _transformar_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforma las columnas del DataFrame"""
        # Eliminar columnas innecesarias
        columnas_a_eliminar = [
            'empresa',
            'tipodecomercializacion',
            'subtipodecomercializacion',
            'pais',
            'indice_tiempo'
        ]
        df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])
        
        # Crear columna fecha
        df['fecha'] = pd.to_datetime(
            df['anio'].astype(str) + '-' + df['mes'].astype(str) + '-01'
        )
        
        # Eliminar columnas año y mes
        df = df.drop(columns=['anio', 'mes'])
        
        # Reordenar: fecha primero
        df.insert(0, 'fecha', df.pop('fecha'))
        
        return df
    
    def _transformar_provincia(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforma y filtra provincias"""
        # Filtrar valores no deseados
        provincias_no_deseadas = ['S/D', 'no aplica', 'Provincia']
        df = df[~df['provincia'].isin(provincias_no_deseadas)]
        
        # Reemplazar nombres por códigos
        df['provincia'] = df['provincia'].replace(self.dict_provincias)
        
        # Filtrar solo Corrientes (código 18)
        df = df[df['provincia'] == 18]
        
        return df
    
    def calcular_suma_por_fecha(self, df: pd.DataFrame) -> float:
        """
        Calcula la suma de combustible para la última fecha disponible.
        
        Args:
            df: DataFrame transformado
            
        Returns:
            Suma total de cantidad para la última fecha
        """
        ultima_fecha = df['fecha'].max()
        df_fecha = df[df['fecha'] == ultima_fecha]
        suma = df_fecha['cantidad'].sum()
        
        self.logger.info(f"Suma calculada para {ultima_fecha}: {suma}")
        return float(suma)




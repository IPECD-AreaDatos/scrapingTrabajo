"""
Validador de datos para el proceso ETL de CBT/CBA.

Valida la calidad y consistencia de los datos antes de cargarlos a la base de datos.
"""

import pandas as pd
import numpy as np
from datetime import datetime


class DataValidator:
    """
    Validador de datos para CBA, CBT y NEA.
    
    Realiza validaciones de:
    - Tipos de datos
    - Rangos válidos
    - Completitud
    - Coherencia temporal
    """
    
    def __init__(self):
        self.errores = []
        self.advertencias = []
    
    def validar_dataframe(self, df):
        """
        Valida el DataFrame completo.
        
        Args:
            df (pd.DataFrame): DataFrame a validar
            
        Returns:
            tuple: (bool, list, list) - (es_valido, errores, advertencias)
        """
        print("[VALIDATE] Iniciando validación de datos...")
        
        self.errores = []
        self.advertencias = []
        
        # Validaciones
        self._validar_columnas_requeridas(df)
        self._validar_tipos_datos(df)
        self._validar_valores_nulos(df)
        self._validar_rangos(df)
        self._validar_coherencia_temporal(df)
        self._validar_tendencias(df)
        
        es_valido = len(self.errores) == 0
        
        if es_valido:
            print(f"[VALIDATE] ✓ Validación exitosa. {len(self.advertencias)} advertencias.")
        else:
            print(f"[VALIDATE] ✗ Validación fallida. {len(self.errores)} errores, {len(self.advertencias)} advertencias.")
        
        return es_valido, self.errores, self.advertencias
    
    def _validar_columnas_requeridas(self, df):
        """Valida que existan todas las columnas requeridas."""
        columnas_requeridas = ['Fecha', 'CBA_Adulto', 'CBT_Adulto', 'CBA_Hogar', 'CBT_Hogar', 'cba_nea', 'cbt_nea']
        
        for col in columnas_requeridas:
            if col not in df.columns:
                self.errores.append(f"Columna requerida '{col}' no encontrada")
    
    def _validar_tipos_datos(self, df):
        """Valida que los tipos de datos sean correctos."""
        if 'Fecha' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['Fecha']):
                self.errores.append("La columna 'Fecha' debe ser de tipo datetime")
        
        columnas_numericas = ['CBA_Adulto', 'CBT_Adulto', 'CBA_Hogar', 'CBT_Hogar', 'cba_nea', 'cbt_nea']
        for col in columnas_numericas:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    self.errores.append(f"La columna '{col}' debe ser numérica")
    
    def _validar_valores_nulos(self, df):
        """Valida la presencia de valores nulos."""
        # Permitir algunos nulos en cba_nea y cbt_nea (datos históricos)
        columnas_criticas = ['Fecha', 'CBA_Adulto', 'CBT_Adulto', 'CBA_Hogar', 'CBT_Hogar']
        
        for col in columnas_criticas:
            if col in df.columns:
                nulos = df[col].isna().sum()
                if nulos > 0:
                    self.errores.append(f"La columna '{col}' tiene {nulos} valores nulos")
        
        # Advertir sobre nulos en NEA
        if 'cba_nea' in df.columns:
            nulos_nea = df['cba_nea'].isna().sum()
            if nulos_nea > len(df) * 0.5:  # Más del 50% nulos
                self.advertencias.append(f"La columna 'cba_nea' tiene {nulos_nea} valores nulos ({nulos_nea/len(df)*100:.1f}%)")
    
    def _validar_rangos(self, df):
        """Valida que los valores estén en rangos razonables."""
        # CBA debe ser menor que CBT
        if 'CBA_Adulto' in df.columns and 'CBT_Adulto' in df.columns:
            invalidos = df[df['CBA_Adulto'] > df['CBT_Adulto']]
            if len(invalidos) > 0:
                self.errores.append(f"Hay {len(invalidos)} filas donde CBA_Adulto > CBT_Adulto")
        
        # Valores deben ser positivos
        columnas_numericas = ['CBA_Adulto', 'CBT_Adulto', 'CBA_Hogar', 'CBT_Hogar', 'cba_nea', 'cbt_nea']
        for col in columnas_numericas:
            if col in df.columns:
                negativos = df[df[col] < 0]
                if len(negativos) > 0:
                    self.errores.append(f"La columna '{col}' tiene {len(negativos)} valores negativos")
    
    def _validar_coherencia_temporal(self, df):
        """Valida la coherencia temporal de los datos."""
        if 'Fecha' not in df.columns:
            return
        
        # Verificar que las fechas estén ordenadas
        if not df['Fecha'].is_monotonic_increasing:
            self.advertencias.append("Las fechas no están ordenadas cronológicamente")
        
        # Verificar duplicados
        duplicados = df['Fecha'].duplicated().sum()
        if duplicados > 0:
            self.errores.append(f"Hay {duplicados} fechas duplicadas")
        
        # Verificar gaps temporales
        df_sorted = df.sort_values('Fecha')
        diff = df_sorted['Fecha'].diff()
        gaps = diff[diff > pd.Timedelta(days=35)]  # Más de un mes
        if len(gaps) > 0:
            self.advertencias.append(f"Hay {len(gaps)} gaps temporales mayores a un mes")
    
    def _validar_tendencias(self, df):
        """Valida que las tendencias sean razonables."""
        if 'CBA_Adulto' not in df.columns or len(df) < 2:
            return
        
        # Calcular variación mensual
        df_sorted = df.sort_values('Fecha').copy()
        df_sorted['var_cba'] = df_sorted['CBA_Adulto'].pct_change()
        
        # Advertir sobre variaciones extremas (>50% mensual)
        extremas = df_sorted[abs(df_sorted['var_cba']) > 0.5]
        if len(extremas) > 0:
            self.advertencias.append(f"Hay {len(extremas)} variaciones mensuales extremas (>50%) en CBA_Adulto")
    
    def generar_reporte(self):
        """
        Genera un reporte de validación.
        
        Returns:
            str: Reporte formateado
        """
        reporte = "\n" + "="*60 + "\n"
        reporte += "REPORTE DE VALIDACIÓN DE DATOS\n"
        reporte += "="*60 + "\n\n"
        
        if len(self.errores) == 0 and len(self.advertencias) == 0:
            reporte += "✓ Todos los datos son válidos\n"
        else:
            if len(self.errores) > 0:
                reporte += f"ERRORES ({len(self.errores)}):\n"
                for i, error in enumerate(self.errores, 1):
                    reporte += f"  {i}. {error}\n"
                reporte += "\n"
            
            if len(self.advertencias) > 0:
                reporte += f"ADVERTENCIAS ({len(self.advertencias)}):\n"
                for i, adv in enumerate(self.advertencias, 1):
                    reporte += f"  {i}. {adv}\n"
        
        reporte += "="*60 + "\n"
        return reporte

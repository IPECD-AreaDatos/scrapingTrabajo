import pandas as pd
import os
import re

class TransformData:
    def __init__(self):
        self.month_map = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
        }

    def parse_periodo(self, filename):
        """Extrae el periodo del nombre del archivo (ej: rf604m-ene25.xls)."""
        filename_lower = filename.lower()
        # Buscar mes y año
        match = re.search(r'-([a-z]+)(\d{2})', filename_lower)
        if match:
            month_str = match.group(1)
            year_short = match.group(2)
            month = self.month_map.get(month_str)
            if month:
                return f"20{year_short}-{month:02d}-01"
        
        # Fallback para nombres largos (enero, etc)
        for m_name, m_num in self.month_map.items():
            if m_name in filename_lower:
                # Buscar año despues del mes
                match_y = re.search(fr'{m_name}(\d{{2}})', filename_lower)
                year = match_y.group(1) if match_y else "26" # Default a 2026 si no hay año
                return f"20{year}-{m_num:02d}-01"
        
        return None

    def process_file(self, file_path):
        """Procesa un archivo Excel y devuelve un DataFrame con el formato de la BD."""
        filename = os.path.basename(file_path)
        periodo = self.parse_periodo(filename)
        if not periodo:
            print(f"No se pudo determinar el periodo para {filename}")
            return None

        print(f"Procesando {filename} (Periodo: {periodo})...")
        
        try:
            xl = pd.ExcelFile(file_path, engine='xlrd')
            df = xl.parse(xl.sheet_names[0], header=None)
        except Exception as e:
            print(f"Error al leer {filename}: {e}")
            return None

        records = []
        current_jurisdiccion = None
        current_tipo_financ = None

        # Partidas de interes
        partidas_interes = [
            'GASTOS EN PERSONAL', 'BIENES DE CONSUMO', 'SERVICIOS NO PERSONALES',
            'BIENES DE USO', 'TRANSFERENCIAS', 'ACTIVOS FINANCIEROS',
            'SERVICIO DE LA DEUDA Y DISMINUCION DE OTROS', 'GASTOS FIGURATIVOS', 'OTROS GASTOS'
        ]

        # Mapeo de columnas de montos (corregido segun inspeccion de filas de datos)
        col_vigente = 25
        col_comprometido = 28
        col_ordenado = 32

        for _, row in df.iterrows():
            row_str = " ".join(str(val) for val in row.values if pd.notna(val))
            
            # 1. Buscar Jurisdiccion
            if "Entidad:" in row_str:
                # Ej: "Entidad:  1 - MINISTERIO DE SEGURIDAD"
                match = re.search(r'Entidad:\s+\d+\s+-\s+(.+)', row_str)
                if match:
                    current_jurisdiccion = match.group(1).strip()
                continue

            # 2. Buscar Tipo Financiamiento (Fuente)
            if "Codigo Fuente:" in row_str:
                # Ej: "Codigo Fuente:  10 -RECURSOS..."
                match = re.search(r'Codigo Fuente:\s+(\d+)', row_str)
                if match:
                    current_tipo_financ = int(match.group(1))
                continue

            # 3. Buscar Partidas y Montos
            for partida in partidas_interes:
                # Comprobamos si la celda de la partida contiene el texto
                # La partida suele estar en la columna 11 segun inspeccion
                if pd.notna(row[11]) and partida in str(row[11]):
                    if not current_jurisdiccion or current_tipo_financ is None:
                        continue

                    # Extraer montos
                    v_vigente = self.clean_monto(row[col_vigente])
                    v_comprometido = self.clean_monto(row[col_comprometido])
                    v_ordenado = self.clean_monto(row[col_ordenado])

                    # Crear un registro por cada estado
                    estados = {
                        "Credito Vigente": v_vigente,
                        "Comprometido": v_comprometido,
                        "Ordenado": v_ordenado
                    }

                    for estado, monto in estados.items():
                        if monto != 0: # Evitar cargar ceros si no es necesario
                            records.append({
                                "periodo": periodo,
                                "jurisdiccion": current_jurisdiccion,
                                "tipo_financ": current_tipo_financ,
                                "partida": partida,
                                "estado": estado,
                                "monto": monto
                            })
                    break # Encontrada la partida en esta fila

        return pd.DataFrame(records)

    def clean_monto(self, val):
        """Limpia y convierte a float valores del Excel."""
        if pd.isna(val):
            return 0.0
        try:
            return float(val)
        except:
            return 0.0

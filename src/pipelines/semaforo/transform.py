import pandas as pd
import datetime
from src.shared.logger import setup_logger
from src.shared.mappings import meses_abbr

logger = setup_logger("semaforo")

def transform_semaforo(df_raw: pd.DataFrame) -> pd.DataFrame:
    try:
        # Convertimos a lista para procesar
        data = df_raw.values.tolist()

        if not data or len(data) < 2:
            logger.warning("⚠️ DataFrame de origen vacío o incompleto.")
            return pd.DataFrame()

        fechas = data[0]
        datos = data[1:]

        max_len = max(len(row) for row in datos)
        fechas = fechas[:max_len]
        datos = [_pad_with_nones(row, max_len) for row in datos]

        columnas = [
            'combustible_vendido', 'patentamiento_0km_auto', 'patentamiento_0km_motocicleta',
            'pasajeros_salidos_terminal_corrientes', 'pasajeros_aeropuerto_corrientes',
            'venta_supermercados_autoservicios_mayoristas', 'exportaciones_aduana_corrientes_dolares',
            'exportaciones_aduana_corrientes_toneladas', 'empleo_privado_registrado_sipa', 'ipicorr'
        ]

        df = pd.DataFrame({'fecha': _convertir_fechas(fechas)})
        for i, nombre_col in enumerate(columnas):
            df[nombre_col] = datos[i]

        for col in columnas:
            df[col] = (
                pd.Series(df[col])
                .astype(str)
                .str.replace('%', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce') / 100
            df[col] = df[col].round(3)

        return df

    except Exception as e:
        logger.error(f"❌ Error al transformar datos: {e}")
        return pd.DataFrame()

def _pad_with_nones(row, max_len):
    return row + [None] * (max_len - len(row))

def _convertir_fechas(fechas):
    fechas_convertidas = []

    for f in fechas:
        try:
            mes_abbr, anio = f.lower().split('-')
            mes = meses_abbr.get(mes_abbr, '01')
            anio = int(anio) + 2000
            fechas_convertidas.append(datetime.datetime(anio, int(mes), 1))
        except Exception as e:
            logger.warning(f"❗ Fecha inválida: {f} → {e}")
            fechas_convertidas.append(None)

    return fechas_convertidas
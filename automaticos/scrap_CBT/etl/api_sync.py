"""Entrega persistente de canastas a ECV/EPH."""
import logging
from contextlib import closing
import os
import sqlite3
import ssl

import certifi
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)
API_URL = 'https://ecv.corrientes.gob.ar/api/create_cbt'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_PATH = os.path.join(BASE_DIR, 'files', 'data', 'envios_ecv.sqlite3')
INTERMEDIATE = os.path.join(BASE_DIR, 'sectigo_intermediate.pem')
# Julio fue confirmado por la API; agosto y septiembre fallaron por TLS.
FIRST_PENDING_MONTH = '2026-08-01'


class VerifiedChainAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        context.load_verify_locations(cafile=INTERMEDIATE)
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)


def sync_api(df, state_path=STATE_PATH, session=None):
    """Encola cada mes y entrega en orden. No reintenta resultados ambiguos."""
    os.makedirs(os.path.dirname(os.path.abspath(state_path)), exist_ok=True)
    client = session or requests.Session()
    if session is None:
        client.mount('https://', VerifiedChainAdapter())
    try:
        with closing(sqlite3.connect(state_path, timeout=30)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS envios (mes TEXT PRIMARY KEY, cba INTEGER NOT NULL, cbt INTEGER NOT NULL, estado TEXT NOT NULL, detalle TEXT)')
            rows = df.copy()
            rows['fecha'] = pd.to_datetime(rows['fecha'])
            rows = rows[rows['fecha'] >= pd.Timestamp(FIRST_PENDING_MONTH)].sort_values('fecha')
            for _, row in rows.iterrows():
                if pd.isna(row['cba_nea']) or pd.isna(row['cbt_nea']):
                    raise ValueError('Faltan valores NEA para ' + str(row['fecha'].date()))
                db.execute('INSERT OR IGNORE INTO envios VALUES (?, ?, ?, ?, NULL)',
                           (row['fecha'].strftime('%Y-%m'), int(row['cba_nea']), int(row['cbt_nea']), 'pendiente'))
            db.commit()
            # Un proceso interrumpido tras iniciar el POST podría haber insertado datos.
            db.execute("UPDATE envios SET estado='revision', detalle='Ejecución interrumpida durante el envío' WHERE estado='enviando'")
            db.commit()
            for mes, cba, cbt, estado in db.execute('SELECT mes,cba,cbt,estado FROM envios WHERE estado != ? ORDER BY mes', ('enviado',)).fetchall():
                if estado == 'revision':
                    raise RuntimeError(f'Envío {mes} requiere revisión antes de reintentar para evitar duplicados')
                db.execute("UPDATE envios SET estado='enviando' WHERE mes=?", (mes,))
                db.commit()
                try:
                    response = client.post(API_URL, data={'anio': int(mes[:4]), 'mes': int(mes[5:]), 'cba': cba, 'cbt': cbt}, timeout=10)
                except requests.exceptions.SSLError as exc:
                    db.execute("UPDATE envios SET estado='pendiente', detalle=? WHERE mes=?", (str(exc), mes))
                    db.commit()
                    raise RuntimeError(f'Falló la conexión TLS para {mes}; se reintentará') from exc
                except requests.exceptions.RequestException as exc:
                    db.execute("UPDATE envios SET estado='revision', detalle=? WHERE mes=?", (str(exc), mes))
                    db.commit()
                    raise RuntimeError(f'Resultado incierto para {mes}; requiere revisión') from exc
                body = response.text.lower()
                if response.status_code in (200, 201) and 'ecv' in body and 'eph' in body and body.count('inserto') >= 2:
                    db.execute("UPDATE envios SET estado='enviado', detalle=? WHERE mes=?", (response.text[:500], mes))
                    db.commit()
                    logger.info('[API] %s actualizado en ECV y EPH (HTTP %s).', mes, response.status_code)
                else:
                    db.execute("UPDATE envios SET estado='revision', detalle=? WHERE mes=?", (f'HTTP {response.status_code}: {response.text[:500]}', mes))
                    db.commit()
                    raise RuntimeError(f'Respuesta no confirmada para {mes} (HTTP {response.status_code}); requiere revisión')
    finally:
        if session is None:
            client.close()

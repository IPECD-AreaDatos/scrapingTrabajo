import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import pandas as pd
import uuid
import datetime

# Importamos tus modelos y el engine
from etl.models import PacienteGold, PacienteGoldFuentes, engine
from etl.field_map import FIELD_MAP, FUENTE_PRIORIDADES

logger = logging.getLogger(__name__)
Session = sessionmaker(bind=engine)

def run_gold_consolidation():
    """
    Lee las tablas fuente (derivaciones, sumar, caps, pof) y consolida 
    en PacienteGold respetando las prioridades de Tony.
    """
    session = Session()
    batch_id = str(uuid.uuid4())[:8]
    
    # Mapeo de tus tablas físicas a las claves del FIELD_MAP
    tablas_fuente = {
        "pof": "v_embarazosdw",
        "sumar": "embarazadas_sumar",
        "derivaciones": "derivaciones_red_obstetricia",
        "caps": "embarazadas_derivadas_alto_riesgo_caps"
    }

    pacientes_pendientes = {}

    try:
        # 1. EXTRACCIÓN DE TABLAS LOCALES
        for fuente_key, nombre_tabla in tablas_fuente.items():
            logger.info(f"Leyendo tabla local: {nombre_tabla}...")
            try:
                df = pd.read_sql_table(nombre_tabla, engine)
                if df.empty: continue
                
                # Agrupamos por DNI para consolidar
                for _, row in df.iterrows():
                    dni = str(row.get('dni')).strip()
                    if not dni or dni == '0': continue
                    
                    if dni not in pacientes_pendientes:
                        pacientes_pendientes[dni] = {}
                    
                    # Guardamos los datos crudos por fuente para este DNI
                    pacientes_pendientes[dni][fuente_key] = row.to_dict()
            except Exception as e:
                logger.error(f"Error leyendo {nombre_tabla}: {e}")

        # 2. CONSOLIDACIÓN Y PRIORIZACIÓN
        logger.info(f"Consolidando {len(pacientes_pendientes)} pacientes únicos...")
        
        for dni, fuentes in pacientes_pendientes.items():
            # Ordenamos las fuentes disponibles para este DNI según la prioridad de Tony
            # POF (1) > WP (2) > SUMAR (3) > Derivaciones (4)
            fuentes_ordenadas = sorted(
                fuentes.keys(), 
                key=lambda f: FUENTE_PRIORIDADES.get(f, 99)
            )
            
            datos_finales = {}
            fuente_principal = fuentes_ordenadas[0]
            
            # Llenamos los campos: si el dato está en una fuente de mayor prioridad, se queda ese[cite: 1]
            for fuente in fuentes_ordenadas:
                for col_origen, col_destino in FIELD_MAP.get(fuente, {}).items():
                    valor = fuentes[fuente].get(col_origen)
                    if valor and pd.notna(valor) and col_destino not in datos_finales:
                        datos_finales[col_destino] = valor

            # 3. GUARDADO EN CAPA GOLD
            if 'fecha_probable_parto' in datos_finales:
                upsert_gold(session, dni, datos_finales, fuente_principal, batch_id)

        session.commit()
        logger.info("=== PROCESO GOLD FINALIZADO CON ÉXITO ===")

    except Exception as e:
        session.rollback()
        logger.error(f"Falla crítica en consolidación: {e}")
    finally:
        session.close()

def upsert_gold(session, dni, datos, fuente_principal, batch_id):
    """Inserta o actualiza en la tabla Gold."""
    fpp = datos.get('fecha_probable_parto')
    
    existing = session.query(PacienteGold).filter_by(dni=dni, fecha_probable_parto=fpp).first()
    
    if existing:
        for k, v in datos.items():
            setattr(existing, k, v)
        existing.batch_id = batch_id
        existing.fuente = fuente_principal
    else:
        nuevo = PacienteGold(
            dni=dni,
            batch_id=batch_id,
            fuente=fuente_principal,
            **datos
        )
        session.add(nuevo)
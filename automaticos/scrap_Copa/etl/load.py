import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Search for .env in current and parent directories
# Using absolute path for more reliability
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.abspath(os.path.join(current_dir, '..', '..', '..', '.env'))
load_dotenv(dotenv_path)

# Database credentials
DB_HOST = os.getenv("HOST_DBB2")
DB_PORT = os.getenv("PORT_DBB2", "5432")
DB_USER = os.getenv("USER_DBB2")
DB_PASS = os.getenv("PASSWORD_DBB2")

# The .env file has 'NAME_DB_DATOS_TABLERO = datos_tablero'
# We'll try to get it, and fallback if needed.
DB_NAME = os.getenv("NAME_DB_DATOS_TABLERO")
if not DB_NAME:
    # Try with space if needed or use default
    DB_NAME = os.getenv("NAME_DB_DATOS_TABLERO ") or "datos_tablero"

# Strip in case it picked up spaces
DB_NAME = DB_NAME.strip()

TABLE_NAME = "copa_recursos_origen_nacional"

COLUMN_MAPPING = {
    "Fecha": "fecha",
    "CFI (Neta de Ley 26075)": "cfi_neta_ley_26075",
    "Financ. Educativo (Ley 26075)": "financ_educativo_ley_26075",
    "SUBTOTAL": "subtotal",
    "Transferencia de Servicios - Educacion": "transf_servicios_educacion",
    "Transferencia de Servicios - Posoco": "transf_servicios_posoco",
    "Transferencia de Servicios - Prosonu": "transf_servicios_prosonu",
    "Transferencia de Servicios - Hospitales": "transf_servicios_hospitales",
    "Transferencia de Servicios - Minoridad": "transf_servicios_minoridad",
    "Transferencia de Servicios - TOTAL": "transf_servicios_total",
    "Imp. Bienes Personales (Ley 24.699)": "imp_bienes_personales_ley_24699",
    "Imp. Bienes Personales (Ley 23.966 Art. 30)": "imp_bienes_personales_ley_23966",
    "Imp. s/ los Activos Fdo. Educativo (Ley 23.906)": "imp_activos_fdo_educativo",
    "I.V.A. (Ley 23.966 Art. 5 Pto. 2)": "iva_ley_23966",
    "Imp. Combustibles (Ley N.23966 Obras de Infraestructura)": "imp_combustibles_infraestructura",
    "Imp. Combustibles (Ley N.23966 Vialidad Provincial)": "imp_combustibles_vialidad",
    "Imp. Combustibles (FO.NA.VI.)": "imp_combustibles_fonavi",
    "Fondo Compensador Deseq.Fisc. Provinciales": "fondo_compensador_deseq_fisc",
    "Reg.Simplif. p/Pequenos Contribuyentes (Ley N.24.977)": "reg_simplif_monotributo",
    "TOTAL Recursos Origen Nacional (1)": "total_recursos_origen_nacional",
    "Compensacion Consenso Fiscal (2)": "compensacion_consenso_fiscal",
    "Total - (1)+(2)": "total_general",
    "Punto Estadistico": "punto_estadistico"
}

def get_engine():
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_to_postgres(csv_path):
    print(f"Loading data from {csv_path} to PostgreSQL...")
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return False
        
    df = pd.read_csv(csv_path)
    
    # 1. Rename columns based on mapping
    # Only rename if the column exists in the DataFrame (some might have been dropped)
    current_mapping = {k: v for k, v in COLUMN_MAPPING.items() if k in df.columns}
    df = df[list(current_mapping.keys())].rename(columns=current_mapping)
    
    # 2. Convert date
    df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
    
    # 3. Connect and Load
    engine = get_engine()
    
    try:
        with engine.begin() as connection:
            # Delete only the specific dates present in the DataFrame
            # This ensures we only "update" the days we have new data for
            dates_to_delete = df['fecha'].dt.strftime('%Y-%m-%d').unique().tolist()
            
            print(f"Syncing {len(dates_to_delete)} dates in PostgreSQL...")
            delete_query = text(f"DELETE FROM {TABLE_NAME} WHERE CAST(fecha AS DATE) IN :dates")
            connection.execute(delete_query, {"dates": tuple(dates_to_delete)})
            
            # Load
            print(f"Inserting {len(df)} rows...")
            df.to_sql(TABLE_NAME, con=connection, if_exists='append', index=False)
            
        print("Data successfully loaded to PostgreSQL.")
        return True
        
    except Exception as e:
        print(f"Error during database load: {e}")
        return False

if __name__ == "__main__":
    # Test path
    test_csv = os.path.join("files", "processed", "consolidado_copa.csv")
    load_to_postgres(test_csv)

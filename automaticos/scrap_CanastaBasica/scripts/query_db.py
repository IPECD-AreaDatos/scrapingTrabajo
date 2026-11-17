import pandas as pd
import os
import sys
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

host = os.getenv('HOST_DBB2')
user = os.getenv('USER_DBB2')
pw = os.getenv('PASSWORD_DBB2')
port = os.getenv('PORT_DBB2', '5432')
db = os.getenv('NAME_DB_CANASTA', 'canasta_basica_super')

engine = create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")

q = text("SELECT * FROM link_productos LIMIT 5")
with engine.connect() as conn:
    df = pd.read_sql(q, conn)
    print(df.to_string())
    
engine.dispose()

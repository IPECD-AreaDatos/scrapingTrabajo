import pandas as pd
import os
import sys
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

host = os.getenv('HOST_DBB1')
user = os.getenv('USER_DBB1')
pw = os.getenv('PASSWORD_DBB1')
db = os.getenv('NAME_DB_CANASTA_V1', 'canasta_basica_supermercados')

engine = create_engine(f"mysql+pymysql://{user}:{pw}@{host}:3306/{db}")

q = text("SELECT * FROM link_productos LIMIT 5")
with engine.connect() as conn:
    df = pd.read_sql(q, conn)
    print(df.to_string())
    
engine.dispose()

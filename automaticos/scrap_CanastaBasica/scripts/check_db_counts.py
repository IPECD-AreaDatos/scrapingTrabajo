import os
import sys
import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

host = os.getenv('HOST_DBB2')
user = os.getenv('USER_DBB2')
pw = os.getenv('PASSWORD_DBB2')
db = os.getenv('NAME_DB_CANASTA', 'canasta_basica_super')

engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:5432/{db}')

q = text("SELECT fecha_extraccion, count(*), sum(case when precio_normal > 0 or precio_descuento > 0 then 1 else 0 end) as validos FROM precios_productos GROUP BY fecha_extraccion ORDER BY fecha_extraccion DESC LIMIT 5")

with engine.connect() as conn:
    df = pd.read_sql(q, conn)
    print(df)

engine.dispose()

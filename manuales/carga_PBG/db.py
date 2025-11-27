# db.py
from __future__ import annotations
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def get_engine(db_name_env_key: str = "NAME_DBB_DATALAKE_ECONOMICO"):
    load_dotenv()
    host = os.getenv("HOST_DBB")
    user = os.getenv("USER_DBB")
    pwd  = os.getenv("PASSWORD_DBB")
    db   = os.getenv(db_name_env_key)
    if not all([host, user, pwd, db]):
        raise RuntimeError("Faltan variables de entorno para la conexión MySQL.")
    url = f"mysql+pymysql://{user}:{pwd}@{host}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

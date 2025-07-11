from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from pymysql import connect
from dotenv import load_dotenv
from json import loads

load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

def sheet_combustible_data():
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def load_to_postgres(csv_path):
    """
    Loads the cleaned data into the PostgreSQL database.
    """
    print(f"Loading data from {csv_path} to database")
    
    # DB_URL = os.getenv("DB_URL")
    # engine = create_engine(DB_URL)
    # df = pd.read_csv(csv_path)
    # df.to_sql('copa_gastos', engine, if_exists='append', index=False)
    return True

def main():
    # load_to_postgres('processed.csv')
    pass

if __name__ == "__main__":
    main()

from etl_modular.etl_modules.sipa.run import run_sipa
from etl_modular.etl_modules.anac.run import run_anac
from etl_modular.etl_modules.combustible.run import run_combustible
from etl_modular.etl_modules.mercado_central.run import run_mercado_central

import os
from dotenv import load_dotenv
load_dotenv()

def main():
    #print("🚀 Ejecutando ETL de SIPA")
    #run_sipa()
    run_combustible()
    #un_anac()
    #run_mercado_central()

if __name__ == '__main__':
    main()

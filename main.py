from etl_modular.etl_modules.sipa.run import run_sipa
from etl_modular.etl_modules.anac.run import run_anac
from etl_modular.etl_modules.combustible.run import run_combustible
from etl_modular.etl_modules.mercado_central.run import run_mercado_central
from etl_modular.etl_modules.dnrpa.run import run_dnrpa
from etl_modular.etl_modules.ripte.run import run_ripte
from etl_modular.etl_modules.ipicorr.run import run_ipicorr
from etl_modular.etl_modules.emae.run import run_emae
from etl_modular.etl_modules.semaforo.run import run_semaforo
from etl_modular.etl_modules.supermercado.run import run_supermercado
from etl_modular.etl_modules.canasta_basica.run import run_canasta_basica

import os
from dotenv import load_dotenv
load_dotenv()

def main():
    #print("🚀 Ejecutando ETL de SIPA")
    #run_sipa()
    #run_combustible()
    #run_anac()
    #run_mercado_central()
    #run_dnrpa()
    #run_ripte()
    #run_ipicorr()
    #run_emae()
    #run_semaforo()
    #run_supermercado()
    run_canasta_basica()
    
if __name__ == '__main__':
    main()

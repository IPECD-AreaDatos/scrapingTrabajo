from etl_modular.etl_modules.sipa.run import run_sipa
from etl_modular.etl_modules.anac.run import run_anac
from etl_modular.etl_modules.combustible.run import run_combustible

def main():
    #print("🚀 Ejecutando ETL de SIPA")
    #run_sipa()
    run_combustible()

if __name__ == '__main__':
    main()

from etl_modular.etl_modules.sipa.run import run_sipa
from etl_modular.etl_modules.anac.run import run_anac

def main():
    #print("🚀 Ejecutando ETL de SIPA")
    #run_sipa()
    run_anac()

if __name__ == '__main__':
    main()

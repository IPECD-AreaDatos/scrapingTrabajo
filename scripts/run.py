# scripts/run.py
import argparse
from src.runner import run_job

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name", help="job registrado, ej: diccionario_clae.load")
    ap.add_argument("--source_path")
    ap.add_argument("--table")
    args = ap.parse_args()

    kwargs = {k:v for k,v in vars(args).items() if k not in ("name",) and v is not None}
    run_job(args.name, **kwargs)

if __name__ == "__main__":
    main()

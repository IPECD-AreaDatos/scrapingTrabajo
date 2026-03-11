import os
import sys
import re
from etl.extract import extract_ron_file, download_file
from etl.transform import main as transform_main
from etl.load import load_to_postgres

def extract_date_from_url(url):
    # internet_diario_mar26_7.xls
    match = re.search(r'internet_diario_([a-z]{3})(\d{2})_', url.lower())
    if match:
        month_str = match.group(1)
        year_short = match.group(2)
        month_map = {
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
        }
        return 2000 + int(year_short), month_map.get(month_str)
    return None, None

def main():
    print("--- Starting RON ETL Process ---")
    
    # Configuration
    URL = "https://www.argentina.gob.ar/economia/sechacienda/asuntosprovinciales/ron"
    RAW_DIR = os.path.join("files", "raw")
    TARGET_PATH = os.path.join(RAW_DIR, "ron_raw.xls")
    PROCESSED_CSV = os.path.join("files", "processed", "consolidado_copa.csv")
    
    # Ensure directories exist
    os.makedirs(RAW_DIR, exist_ok=True)
    
    try:
        # 1. Extract
        print("[1/3] Extraction phase...")
        file_url = extract_ron_file(URL, None)
        
        if not file_url:
            print("Error: Could not find the download link on the page.")
            sys.exit(1)
            
        download_file(file_url, TARGET_PATH)
        print(f"Extraction successful: {TARGET_PATH}")
        
        # Determine date context
        year, month = extract_date_from_url(file_url)
        
        # 2. Transform
        print(f"[2/3] Transformation phase (Year: {year}, Month: {month})...")
        transform_main(input_path=TARGET_PATH, year=year, month=month)
        
        # 3. Load
        print("[3/3] Load phase...")
        success = load_to_postgres(PROCESSED_CSV)
        
        if success:
            print("--- ETL Process Finished Successfully ---")
            # Cleanup: Remove files directory and its contents to avoid leaving residues on the server
            import shutil
            if os.path.exists("files"):
                print("Cleaning up temporary files...")
                shutil.rmtree("files")
                print("Cleanup complete.")
        else:
            print("--- ETL Process Finished with Load Errors ---")
            sys.exit(1)
            
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

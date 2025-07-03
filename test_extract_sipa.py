from etl_modular.etl_modules.sipa.extract import extract_sipa_data
from etl_modular.etl_modules.sipa.transform import transform_sipa_data

ruta = extract_sipa_data()
df = transform_sipa_data(ruta)
print(df.head())

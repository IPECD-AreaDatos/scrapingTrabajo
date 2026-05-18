import pandas as pd
import re
import os

def extraer_todo_el_dump(archivo_sql, archivo_salida):
    print(f"🚀 Iniciando procesamiento de {archivo_sql}...")
    
    # Regex para capturar los valores entre paréntesis
    regex_valores = re.compile(r"\((.*?)\)")
    
    # Diccionario para almacenar los datos
    base_datos = {}
    
    # Tu lista de 58 tablas (aseguramos que estén todas en minúsculas para comparar)
    tablas_objetivo = [
        'ddjj_personas', 'productores', 'agricultura', 'bovinos', 
        'establecimientos', 'adremas', 'afecta_forestal', 
        'afecta_ganaderia', 'afecta_otras', 'analisisotrasmejoras', 'analisisovinos',
        'cultivos', 'departamentos', 'apicultura', 'avicultura', 'cultivosestado',
        'cultivosprecios', 'cultivostipo', 'ddjj_personas_temp', 'documentacion',
        'domicilios', 'forestacion', 'forestalprecios', 'fotos', 'ganaderiaprecios',
        'get_adremas', 'localidades', 'menu_admin', 'op', 'otrasprecios', 'ovinos',
        'parajes', 'perdidas_invernaculos', 'perdidas_invernaculos_precios', 'perdidas_mejoras',
        'perdidas_plurianuales', 'perdidas_plurianuales_precios', 'permisos_admin', 
        'permisos_submenu_admin', 'ponderaciones_ddjj', 'porcinos', 'productos', 'productostipo',
        'productos_bkp', 'provincias', 'resoluciones', 'rubro_tipos', 'seguro_agricola',
        'submenu_admin', 'tipoactividad', 'tipodocumento', 'tipojuridico', 'tipotenencia',
        'unidades_medida', 'usuarios_notix', 'vw_productores', 'vw_productos', 'vw_productos_tipo'
    ]

    with open(archivo_sql, 'r', encoding='utf-8', errors='ignore') as f:
        tabla_actual = None
        for linea in f:
            # Detectamos el INSERT INTO
            match_insert = re.search(r"INSERT INTO\s+`?(\w+)`?", linea, re.IGNORECASE)
            
            if match_insert:
                nombre_t = match_insert.group(1).lower()
                # Solo procesamos si está en nuestra lista de 58
                if nombre_t in tablas_objetivo:
                    tabla_actual = nombre_t
                    if tabla_actual not in base_datos:
                        base_datos[tabla_actual] = []
                else:
                    tabla_actual = None
            
            if tabla_actual:
                encontrados = regex_valores.findall(linea)
                for item in encontrados:
                    # Split avanzado para no romper campos con comas internas
                    valores = [v.strip().strip("'").strip('"') for v in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", item)]
                    base_datos[tabla_actual].append(valores)
            
            if ';' in linea:
                tabla_actual = None

    print("\n📊 Resumen de registros encontrados:")
    
    # Escritura en Excel
    with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
        for nombre_tabla in tablas_objetivo:
            registros = base_datos.get(nombre_tabla, [])
            
            if registros:
                df = pd.DataFrame(registros)
                # Si el nombre de la tabla es largo, Excel lo corta a 31
                sheet_name = nombre_tabla[:31]
                
                try:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"✅ {nombre_tabla}: {len(registros)} filas")
                except Exception as e:
                    print(f"❌ Error al guardar {nombre_tabla}: {e}")
            else:
                # Opcional: imprimir cuáles tablas no tenían datos en el SQL
                pass

    print(f"\n✨ ¡Misión cumplida! El Excel '{archivo_salida}' está listo con tus 58 solapas.")

# Rutas
ruta_sql = '/home/usuario/Escritorio/scrapingTrabajo/manuales/scrap_emerg/emergenc_base.sql'
nombre_excel = 'Base_Completa_Emergencia_58_Tablas.xlsx'

extraer_todo_el_dump(ruta_sql, nombre_excel)
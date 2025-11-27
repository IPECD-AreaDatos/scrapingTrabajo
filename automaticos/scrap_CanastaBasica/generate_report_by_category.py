"""
Generador de Reporte: Productos por Categoría
Lee datos de CSV y genera un Excel organizado por categorías

Uso:
    python generate_report_by_category.py --ultimo
    python generate_report_by_category.py --csv archivo.csv
"""
import os
import sys
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Directorios base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE_DIR, 'files')
os.makedirs(FILES_DIR, exist_ok=True)

# Agregar directorio actual al path
sys.path.insert(0, BASE_DIR)

from config.categorias import obtener_categoria

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportePorCategoria:
    """Genera reporte Excel de productos agrupados por categoría"""
    
    def __init__(self, datos_df: pd.DataFrame):
        self.df = datos_df.copy()
        self.files_dir = FILES_DIR
        self._agregar_categorias()
    
    def _agregar_categorias(self):
        """Agrega columna de categoría a cada producto"""
        logger.info("[REPORTE] Asignando categorías a productos...")
        
        self.df['categoria'] = self.df['producto_nombre'].apply(obtener_categoria)
        
        # Mostrar resumen de categorías
        categorias_count = self.df['categoria'].value_counts()
        logger.info("[REPORTE] Productos por categoría:")
        for categoria, count in categorias_count.items():
            logger.info(f"  - {categoria}: {count} productos")
    
    def generar_excel(self, output_file: str = None) -> str:
        """Genera archivo Excel con productos agrupados por categoría"""
        if self.df.empty:
            logger.error("[ERROR] No hay datos para generar reporte")
            return None
        
        if output_file is None:
            fecha = datetime.now().strftime("%Y%m%d_%H%M")
            output_file = f"productos_por_categoria_{fecha}.xlsx"
        
        if not os.path.isabs(output_file):
            output_file = os.path.join(self.files_dir, output_file)
        
        logger.info(f"[REPORTE] Generando Excel: {output_file}")
        
        # Ordenar por categoría y producto
        self.df = self.df.sort_values(['categoria', 'producto_nombre', 'supermercado'])
        
        # Verificar si openpyxl está instalado
        try:
            from openpyxl import load_workbook
        except ImportError:
            logger.error("[ERROR] openpyxl no está instalado. Instale con: pip install openpyxl")
            return None
        
        # Crear Excel con múltiples hojas
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 1. Hoja Resumen
            self._crear_hoja_resumen(writer)
            
            # 2. Hoja Todos los Productos
            self._crear_hoja_todos(writer)
            
            # 3. Hoja por cada categoría
            categorias = sorted(self.df['categoria'].unique())
            for categoria in categorias:
                self._crear_hoja_categoria(writer, categoria)
        
        logger.info(f"[OK] Reporte generado exitosamente: {output_file}")
        return output_file
    
    def _crear_hoja_resumen(self, writer):
        """Crea hoja de resumen con estadísticas por categoría"""
        resumen = []
        
        for categoria in sorted(self.df['categoria'].unique()):
            df_cat = self.df[self.df['categoria'] == categoria]
            
            # Contar productos únicos
            productos_unicos = df_cat['producto_nombre'].nunique()
            
            # Contar supermercados
            supermercados = df_cat['supermercado'].nunique()
            
            # Precio promedio (solo productos con precio)
            df_con_precio = df_cat[df_cat['precio_normal'].notna() & (df_cat['precio_normal'] != '')]
            precio_promedio = None
            if not df_con_precio.empty:
                try:
                    precios = pd.to_numeric(df_con_precio['precio_normal'], errors='coerce')
                    precio_promedio = precios.mean()
                except:
                    pass
            
            resumen.append({
                'Categoría': categoria,
                'Productos Únicos': productos_unicos,
                'Total Registros': len(df_cat),
                'Supermercados': supermercados,
                'Precio Promedio': f"${precio_promedio:.2f}" if precio_promedio else "N/A"
            })
        
        df_resumen = pd.DataFrame(resumen)
        df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
        
        # Ajustar ancho de columnas
        worksheet = writer.sheets['Resumen']
        for idx, col in enumerate(df_resumen.columns):
            max_length = max(
                df_resumen[col].astype(str).map(len).max(),
                len(col)
            ) + 2
            col_letter = chr(65 + idx) if idx < 26 else chr(65 + idx // 26 - 1) + chr(65 + idx % 26)
            worksheet.column_dimensions[col_letter].width = min(max_length, 30)
    
    def _crear_hoja_todos(self, writer):
        """Crea hoja con todos los productos"""
        # Seleccionar columnas relevantes
        columnas = [
            'categoria', 'producto_nombre', 'nombre', 'supermercado',
            'precio_normal', 'precio_descuento', 'precio_por_unidad',
            'unidad_medida', 'peso', 'descuentos', 'fecha_extraccion'
        ]
        
        columnas_disponibles = [col for col in columnas if col in self.df.columns]
        df_todos = self.df[columnas_disponibles].copy()
        
        # Renombrar columnas para mejor presentación
        renombres = {
            'categoria': 'Categoría',
            'producto_nombre': 'Producto (Sheets)',
            'nombre': 'Nombre (Sitio)',
            'supermercado': 'Supermercado',
            'precio_normal': 'Precio Normal',
            'precio_descuento': 'Precio Descuento',
            'precio_por_unidad': 'Precio por Unidad',
            'unidad_medida': 'Unidad',
            'peso': 'Peso',
            'descuentos': 'Descuentos',
            'fecha_extraccion': 'Fecha Extracción'
        }
        
        df_todos = df_todos.rename(columns=renombres)
        df_todos.to_excel(writer, sheet_name='Todos los Productos', index=False)
        
        # Ajustar ancho de columnas
        worksheet = writer.sheets['Todos los Productos']
        for idx, col in enumerate(df_todos.columns):
            max_length = max(
                df_todos[col].astype(str).map(len).max(),
                len(col)
            ) + 2
            col_letter = chr(65 + idx) if idx < 26 else chr(65 + idx // 26 - 1) + chr(65 + idx % 26)
            worksheet.column_dimensions[col_letter].width = min(max_length, 50)
    
    def _crear_hoja_categoria(self, writer, categoria: str):
        """Crea hoja para una categoría específica"""
        df_cat = self.df[self.df['categoria'] == categoria].copy()
        
        # Seleccionar columnas relevantes
        columnas = [
            'producto_nombre', 'nombre', 'supermercado',
            'precio_normal', 'precio_descuento', 'precio_por_unidad',
            'unidad_medida', 'peso', 'descuentos', 'fecha_extraccion', 'url'
        ]
        
        columnas_disponibles = [col for col in columnas if col in df_cat.columns]
        df_cat = df_cat[columnas_disponibles].copy()
        
        # Renombrar columnas
        renombres = {
            'producto_nombre': 'Producto (Sheets)',
            'nombre': 'Nombre (Sitio)',
            'supermercado': 'Supermercado',
            'precio_normal': 'Precio Normal',
            'precio_descuento': 'Precio Descuento',
            'precio_por_unidad': 'Precio por Unidad',
            'unidad_medida': 'Unidad',
            'peso': 'Peso',
            'descuentos': 'Descuentos',
            'fecha_extraccion': 'Fecha Extracción',
            'url': 'URL'
        }
        
        df_cat = df_cat.rename(columns=renombres)
        
        # Limitar nombre de hoja (Excel tiene límite de 31 caracteres)
        nombre_hoja = categoria[:31] if len(categoria) <= 31 else categoria[:28] + "..."
        
        df_cat.to_excel(writer, sheet_name=nombre_hoja, index=False)
        
        # Ajustar ancho de columnas
        worksheet = writer.sheets[nombre_hoja]
        for idx, col in enumerate(df_cat.columns):
            max_length = max(
                df_cat[col].astype(str).map(len).max(),
                len(col)
            ) + 2
            col_letter = chr(65 + idx) if idx < 26 else chr(65 + idx // 26 - 1) + chr(65 + idx % 26)
            worksheet.column_dimensions[col_letter].width = min(max_length, 50)


def generar_desde_csv(csv_file: str) -> str:
    """Genera reporte desde un archivo CSV"""
    csv_path = Path(csv_file)
    if not csv_path.is_absolute():
        potential = Path.cwd() / csv_path
        if potential.exists():
            csv_path = potential
        else:
            csv_path = Path(FILES_DIR) / csv_path.name
    
    logger.info(f"[REPORTE] Leyendo datos desde CSV: {csv_path}")
    
    if not csv_path.exists():
        logger.error(f"[ERROR] Archivo no encontrado: {csv_path}")
        return None
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"[OK] {len(df)} registros cargados desde CSV")
        
        # Verificar que tenga las columnas necesarias
        if 'producto_nombre' not in df.columns:
            logger.error("[ERROR] El CSV no tiene la columna 'producto_nombre'")
            return None
        
        reporte = ReportePorCategoria(df)
        return reporte.generar_excel()
    
    except Exception as e:
        logger.error(f"[ERROR] Error leyendo CSV: {e}", exc_info=True)
        return None


def buscar_ultimo_csv() -> str:
    """Busca el último CSV generado en la carpeta files"""
    csv_files = sorted(Path(FILES_DIR).glob('canasta_basica_completa_*.csv'), reverse=True)
    if csv_files:
        return str(csv_files[0])
    return None


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Genera reporte Excel de productos agrupados por categoría',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python generate_report_by_category.py --ultimo
  python generate_report_by_category.py --csv canasta_basica_completa_20251126_1430.csv
        """
    )
    parser.add_argument('--csv', type=str, help='Archivo CSV con datos de productos')
    parser.add_argument('--ultimo', action='store_true', help='Usar último CSV generado')
    
    args = parser.parse_args()
    
    csv_file = None
    
    if args.csv:
        csv_file = args.csv
    elif args.ultimo:
        csv_file = buscar_ultimo_csv()
        if csv_file:
            logger.info(f"[REPORTE] Usando último CSV encontrado: {csv_file}")
        else:
            logger.error("[ERROR] No se encontraron archivos CSV con patrón 'canasta_basica_completa_*.csv'")
            sys.exit(1)
    else:
        # Por defecto, buscar último CSV
        csv_file = buscar_ultimo_csv()
        if csv_file:
            logger.info(f"[REPORTE] Usando último CSV encontrado: {csv_file}")
        else:
            logger.error("[ERROR] No se encontró ningún CSV. Use --csv para especificar archivo")
            logger.info("[INFO] Uso: python generate_report_by_category.py --csv archivo.csv")
            sys.exit(1)
    
    # Generar reporte
    resultado = generar_desde_csv(csv_file)
    
    if resultado:
        logger.info(f"[OK] Reporte generado exitosamente: {resultado}")
    else:
        logger.error("[ERROR] No se pudo generar el reporte")
        sys.exit(1)



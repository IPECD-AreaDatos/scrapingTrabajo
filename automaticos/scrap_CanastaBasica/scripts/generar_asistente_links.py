import os
import sys
import pandas as pd
import urllib.parse
import re

def clean_slug(url):
    """Extrae palabras de búsqueda desde el final de la URL."""
    try:
        # Remover /p si termina en eso
        if url.endswith('/p'):
            url = url[:-2]
        
        # Obtener la última parte de la url
        slug = url.strip('/').split('/')[-1]
        
        # Remover IDs numéricos del final (muy comunes en Dia y Masonline, ej: -12345)
        slug = re.sub(r'-\d+$', '', slug)
        
        # Reemplazar guiones y guiones bajos por espacios
        search_term = slug.replace('_', ' ').replace('-', ' ')
        
        # Limpiar
        return search_term.strip().lower()
    except:
        return ""

def get_search_url(url, search_term):
    """Devuelve la URL de búsqueda según el supermercado."""
    encoded_term = urllib.parse.quote(search_term)
    
    if 'delimart.com.ar' in url:
        return f"https://www.delimart.com.ar/catalogsearch/result/?q={encoded_term}"
    elif 'masonline.com.ar' in url:
        return f"https://www.masonline.com.ar/{encoded_term}?_q={encoded_term}&map=ft"
    elif 'supermercadosdia.com.ar' in url:
        return f"https://diaonline.supermercadosdia.com.ar/{encoded_term}?_q={encoded_term}&map=ft"
    
    return f"https://www.google.com/search?q={encoded_term}"

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv = os.path.join(base_dir, 'files', 'links_a_modificar.csv')
    output_html = os.path.join(base_dir, 'files', 'reparador_links.html')
    
    if not os.path.exists(input_csv):
        print(f"No se encontró el archivo: {input_csv}")
        return
        
    df = pd.read_csv(input_csv)
    
    if df.empty:
        print("El archivo de links a modificar está vacío.")
        return
        
    # Eliminar duplicados por URL por si acaso
    df = df.drop_duplicates(subset=['url'])
    
    rows_html = ""
    for idx, row in df.iterrows():
        old_url = row.get('url', '')
        if not isinstance(old_url, str) or not old_url:
            continue
            
        error_type = row.get('error_type', 'Desconocido')
        fecha = row.get('fecha_deteccion', '')
        
        search_term = clean_slug(old_url)
        search_url = get_search_url(old_url, search_term)
        
        rows_html += f"""
        <tr>
            <td class="td-old-url" style="word-break: break-all; font-size: 0.85em;"><a href="{old_url}" target="_blank">{old_url}</a></td>
            <td>{search_term}</td>
            <td style="text-align:center;"><a href="{search_url}" target="_blank" class="btn btn-search">🔍 Buscar en Súper</a></td>
            <td><input type="text" class="new-url-input" placeholder="Pegar nueva URL aquí..." style="width: 100%; padding: 8px;"></td>
        </tr>
        """
        
    html_template = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Asistente Reparador de Links - Canasta Básica</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7f6; color: #333; margin: 0; padding: 20px; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; text-align: center; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #2c3e50; color: white; position: sticky; top: 0; }}
            tr:hover {{ background-color: #f1f1f1; }}
            .btn {{ padding: 8px 12px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; text-decoration: none; display: inline-block; }}
            .btn-search {{ background-color: #3498db; color: white; font-size: 0.9em; }}
            .btn-search:hover {{ background-color: #2980b9; }}
            .btn-export {{ background-color: #27ae60; color: white; padding: 12px 24px; font-size: 1.1em; display: block; margin: 20px auto; width: fit-content; }}
            .btn-export:hover {{ background-color: #2ecc71; }}
            .header-info {{ background-color: #e8f4f8; padding: 15px; border-left: 4px solid #3498db; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🛠️ Asistente de Reparación de Links</h1>
            <div class="header-info">
                <p><strong>Instrucciones:</strong></p>
                <ol>
                    <li>Haz clic en <strong>"Buscar en Súper"</strong> para abrir la tienda con el producto sugerido.</li>
                    <li>Navega en la pestaña nueva y copia la URL correcta del producto.</li>
                    <li>Pega la nueva URL en la columna <strong>"Nueva URL"</strong>.</li>
                    <li>Cuando termines, haz clic en el botón <strong>"Exportar Correcciones"</strong> al final de la página.</li>
                </ol>
            </div>
            
            <table id="linksTable">
                <thead>
                    <tr>
                        <th>URL Rota (Actual)</th>
                        <th>Término Detectado</th>
                        <th>Buscador Automático</th>
                        <th>Nueva URL (Pegar aquí)</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
            
            <button class="btn btn-export" onclick="exportToCSV()">💾 Exportar Correcciones</button>
        </div>

        <script>
            function exportToCSV() {{
                const table = document.getElementById('linksTable');
                const rows = table.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
                
                let csvContent = "viejo_link,nuevo_link\\n";
                let count = 0;
                
                for (let i = 0; i < rows.length; i++) {{
                    const oldUrl = rows[i].querySelector('.td-old-url a').innerText.trim();
                    const newUrl = rows[i].querySelector('.new-url-input').value.trim();
                    
                    if (newUrl && newUrl !== "") {{
                        csvContent += `"${{oldUrl}}","${{newUrl}}"\\n`;
                        count++;
                    }}
                }}
                
                if (count === 0) {{
                    alert("No has ingresado ninguna nueva URL para exportar.");
                    return;
                }}
                
                const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
                const link = document.createElement("a");
                const url = URL.createObjectURL(blob);
                link.setAttribute("href", url);
                link.setAttribute("download", "links_corregidos.csv");
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }}
        </script>
    </body>
    </html>
    """
    
    with open(output_html, 'w', encoding='utf-8-sig') as f:
        f.write(html_template)
        
    print(f"Asistente generado exitosamente en:\n{output_html}")
    print(f"Total de links a revisar: {len(df)}")

if __name__ == "__main__":
    main()

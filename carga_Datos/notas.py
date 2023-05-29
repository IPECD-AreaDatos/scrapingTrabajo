workbook = xlrd.open_workbook(file_path)
    sheet = workbook.sheet_by_index(0)  # Suponiendo que la fila se encuentra en la primera hoja (índice 0)
    target_row_index = 0  # Índice de la fila específica que deseas obtener (por ejemplo, fila 1)
    target_row_values = sheet.row_values(target_row_index)

    # Establecer la conexión a la base de datos
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Estadistica123',
        database='nombre_base_de_datos'
    )

    # Generar la consulta SQL para la inserción de datos
    insert_query = f"INSERT INTO {table_name} ({target_column}) VALUES (%s)"

    # Ejecutar la consulta para cada valor de la fila
    for value in target_row_values:
        conn.cursor().execute(insert_query, (value,))
    
    conn.commit()

    # Cerrar la conexión a la base de datos
    conn.close()
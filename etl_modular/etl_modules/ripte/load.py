from datetime import datetime, timedelta
import calendar
from etl_modular.utils.db import ConexionBaseDatos
#from .informe import InformeRipte
from datetime import date

def load_ripte_data(df, host, user, password, database):
    print("💾 Cargando datos históricos de RIPTE...")
    conexion = ConexionBaseDatos(host, user, password, database)
    conexion.connect_db()

    exito = conexion.load_if_newer(df, table_name="ripte", date_column="fecha")

    conexion.close_connections()

    if exito:
        print("✅ Datos cargados en la tabla RIPTE")
    else:
        print("⚠️ No se encontraron datos nuevos")
    return exito

def load_latest_ripte_value(valor, host, user, password, database):
    print("📤 Cargando último valor de RIPTE en la base...")

    conexion = ConexionBaseDatos(host, user, password, database)
    conexion.connect_db()
    cursor = conexion.cursor

    cursor.execute("SELECT fecha, valor FROM ripte ORDER BY fecha DESC LIMIT 1")
    ultima_fecha, valor_bd = cursor.fetchone()

    if abs(valor - valor_bd) < 100:
        print("⚠️ Valor similar, no se carga")
        conexion.close_connections()
        return False

    nueva_fecha = ultima_fecha + timedelta(days=calendar.monthrange(ultima_fecha.year, ultima_fecha.month)[1])
    fecha_actual = date.today()
    cursor.execute("INSERT INTO ripte (fecha, valor) VALUES (%s, %s)", (nueva_fecha, valor))
    conexion.conn.commit()
    conexion.close_connections()

    #InformeRipte(host, user, password, database).enviar_mensajes(fecha_actual, valor, valor_bd)

    print(f"✅ Valor actualizado en la tabla RIPTE: {nueva_fecha} - {valor}")
    return True
    
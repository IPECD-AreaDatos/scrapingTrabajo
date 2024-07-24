# main.py
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import os

# Configuración de la aplicación
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://estadistica:Estadistica2024!!@54.94.131.196/datalake_economico'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicialización de la base de datos
db = SQLAlchemy(app)

# Definición del modelo para la tabla `ripte`
class Combustible(db.Model):
    __tablename__ = 'combustible'
    fecha = db.Column(db.Date, primary_key=True)
    producto = db.Column(db.String(100), nullable=False)
    provincia = db.Column(db.Integer, nullable=False)
    cantidad = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f'<Combustible {self.fecha}>'

# Definición del modelo para la tabla `otra_tabla`
class Dnrpa(db.Model):
    __tablename__ = 'dnrpa'
    fecha = db.Column(db.Date, primary_key=True)
    id_provincia_indec = db.Column(db.Integer, nullable=False)
    id_vehiculo = db.Column(db.Integer, nullable=False)
    cantidad = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return f'<DNRPA {self.fecha}>'


# Ruta de la API para obtener los datos de la tabla `ripte`
@app.route('/combustible', methods=['GET'])
def get_ripte():
    combustible_records = Combustible.query.all()
    return jsonify([{'fecha': record.fecha, 'valor': record.valor} for record in combustible_records])

# Ruta de la API para obtener los datos de la tabla `otra_tabla`
@app.route('/dnrpa', methods=['GET'])
def get_otra_tabla():
    dnrpa_records = Dnrpa.query.all()
    return jsonify([{'fecha': record.fecha, 'id:provincia_indec': record.id_provincia_indec, 'id_vehiculo': record.id_vehiculo, 'cantidad': record.cantidad} for record in dnrpa_records])


if __name__ == '__main__':
    app.run(debug=True)

from flask import Flask, jsonify
import sqlite3
from utils.data_operations import check_header, preprocess_data, read_csv
from database.sqlite import df  

app = Flask(__name__)

@app.route('/weather')
def all_weather_data():
    colname_date, _, _, _ = check_header(df)

    # Create a new SQLite connection within the route
    with sqlite3.connect('database/db.db') as conn:
        query = f'''
            SELECT *
            FROM weather
        '''
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        all_data = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Chame preprocess_data após obter os dados do banco de dados
    all_data = preprocess_data(all_data)
    print("\n")
    print(all_data[0])
    print("\n")

    return jsonify(all_data)


@app.route('/weather/<city>')
def weather(city):
    colname_date, _, _, _ = check_header(df)

    # Create a new SQLite connection within the route
    with sqlite3.connect('database/db.db') as conn:
        query = f'''
            SELECT {colname_date}, temperatura, umidade, precipitacao
            FROM clima
            WHERE cidade COLLATE NOCASE = :city
        '''
        dados = conn.execute(query, {'city': city}).fetchall()

    return jsonify(dados)
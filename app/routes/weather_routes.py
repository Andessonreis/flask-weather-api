from flask import Blueprint, jsonify, request
from app.utils.data_operations import check_header, preprocess_data, read_csv, save_to_database
import sqlite3

weather_blueprint = Blueprint('weather', __name__)

df = read_csv("app/data/INMET_CO_DF_A001_BRASILIA_01-01-2023_A_31-12-2023.CSV")
save_to_database(df, 'weather')

colname_date, colname_hour, ts_format, temp_format = check_header(df)

@weather_blueprint.route('/')
def all_weather_data():

    with sqlite3.connect('db.db') as conn:
        query = f'''
            SELECT *
            FROM weather
        '''
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]

    all_data = preprocess_data(data, colname_date)

    print(f"Column name for date: {colname_date}")

    return jsonify(all_data)


@weather_blueprint.route('/city')
def city_weather_data():
    city_name = request.args.get('cidade')

    with sqlite3.connect('db.db') as conn:
        query = f'''
            SELECT *
            FROM weather
            WHERE cidade = ?
        '''
        cursor = conn.execute(query, (city_name,))
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]

    city_data = preprocess_data(data, colname_date)

    return jsonify(city_data)

from flask import Blueprint, jsonify
from utils.data_operations import check_header, preprocess_data, read_csv, save_to_database
import sqlite3

weather_blueprint = Blueprint('weather', __name__)

@weather_blueprint.route('/')
def all_weather_data():
    df = read_csv("data/INMET_CO_DF_A001_BRASILIA_01-01-2023_A_31-12-2023.CSV")
    save_to_database(df, 'weather')

    colname_date, colname_hour, ts_format, temp_format = check_header(df)

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

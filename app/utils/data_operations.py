import pandas as pd
import re
from typing import List, Tuple
import sqlite3

def read_csv(filename: str) -> pd.DataFrame:
    '''
    Read CSV and create a dataframe, extracting "city" from the filename.

    Parameters:
    - filename (str): The path to the CSV file.

    Returns:
    - pd.DataFrame: The dataframe containing the CSV data with an added "cidade" column.
    '''
    city = extract_city_from_filename(filename)
    df = pd.read_csv(filename, encoding='iso-8859-1', decimal=',', delimiter=';', skiprows=8)
    df['Cidade'] = city
    return df

def extract_city_from_filename(filename: str) -> str:
    '''
    Extract city name from the filename.

    Parameters:
    - filename (str): The name of the file.

    Returns:
    - str: The extracted city name.
    '''
    pattern = re.compile(r'_[^_]+_[^_]+_[^_]+_([^_]+)_')
    match = re.search(pattern, filename)
    return match.group(1) if match else ''

def check_header(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    '''
    Check header formats and return relevant column names and formats.

    Parameters:
    - df (pd.DataFrame): The dataframe to check.

    Returns:
    - tuple: A tuple containing column names and formats.
    '''
    name1 = 'DATA (YYYY-MM-DD)'
    name2 = 'Data'
    colname_hour = 'HORA (UTC)'
    temp_format = 'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)'
    
    if name1 in df.columns:
        colname_date = name1
        ts_format = '%Y-%m-%d %H:%M'
    elif name2 in df.columns:
        colname_date = name2
        if 'Hora UTC' in df.columns:
            colname_hour = 'Hora UTC'
        ts_format = '%Y/%m/%d %H%M UTC'
    else:
        print('Check header format manually')
        exit()
    
    return colname_date, colname_hour, ts_format, temp_format

def preprocess_data(data: List[dict], colname_date: str) -> List[dict]:
    '''
    Clean and preprocess data, selecting essential fields and formatting date and time.

    Parameters:
    - data (list): List of dictionaries representing data points.
    - colname_date (str): The column name for the date.

    Returns:
    - list: List of dictionaries containing processed data points.
    '''
    necessary_columns = [
        "Cidade",
        colname_date,
        "Hora UTC",
        "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
        "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
        "UMIDADE RELATIVA DO AR, HORARIA (%)",
        "VENTO, DIREÇÃO HORARIA (gr) (° (gr))",
        "VENTO, VELOCIDADE HORARIA (m/s)"
    ]

    processed_data = []

    for data_point in data:
        processed_point = {key: data_point[key] for key in necessary_columns}
        
        # Convert null values to None
        for key, value in processed_point.items():
            if pd.isna(value):
                processed_point[key] = None

        # Format date and time
        processed_point[colname_date] = pd.to_datetime(processed_point[colname_date]).strftime('%Y-%m-%d')
        processed_point['Hora UTC'] = processed_point['Hora UTC'].replace(' UTC', '')

        processed_data.append(processed_point)

    return processed_data

def save_to_database(df, table_name):
    '''
    Save DataFrame to a SQLite database table.

    Parameters:
    - df (pd.DataFrame): The dataframe to be saved.
    - table_name (str): The name of the table in the SQLite database.
    '''
    conn = sqlite3.connect('db.db')

    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite Error: {e}")
        conn.rollback()
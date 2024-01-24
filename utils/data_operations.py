import pandas as pd
import os

def read_csv(filename):
    '''
    Read CSV and create a dataframe, extracting "city" from the filename.

    Parameters:
    - filename (str): The path to the CSV file.

    Returns:
    - pd.DataFrame: The dataframe containing the CSV data with an added "cidade" column.
    '''
    city = extract_city_from_filename(filename)
    df = pd.read_csv(filename, encoding='iso-8859-1', decimal=',', delimiter=';', skiprows=8)
    df['cidade'] = city
    return df

def extract_city_from_filename(filename):
    '''
    Extract city name from the filename.

    Parameters:
    - filename (str): The name of the file.

    Returns:
    - str: The extracted city name.
    '''
    city = os.path.splitext(os.path.basename(filename))[0].split('_')[-1]
    return city

def check_header(df):
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

def preprocess_data(data):
    '''
    Clean and preprocess data, converting null values and formatting date and time.

    Parameters:
    - data (list): List of dictionaries representing data points.
    '''
    unnecessary_columns = ['Unnamed: 19']
    
    # Remove unnecessary columns
    for data_point in data:
        for col in unnecessary_columns:
            data_point.pop(col, None)

    # Convert null values to None
    for data_point in data:
        for key, value in data_point.items():
            if pd.isna(value):
                data_point[key] = None

    # Format date and time
    for data_point in data:
        data_point['Data'] = pd.to_datetime(data_point['Data']).strftime('%Y-%m-%d')
        data_point['Hora UTC'] = data_point['Hora UTC'].replace(' UTC', '')

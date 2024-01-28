from utils.data_operations import read_csv, save_to_database
from routes.weather_routes import weather_blueprint
from flask import Flask

app = Flask(__name__)
app.register_blueprint(weather_blueprint, url_prefix='/weather')

if __name__ == '__main__':
    app.run(port=5000,debug=True)

 
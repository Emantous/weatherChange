from dotenv import load_dotenv
import os, sqlite3, requests
from datetime import date
import pandas as pd

def extract(city, weather_date, API_KEY):
    lat_lon = (requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city[1]},{city[3]}&appid={API_KEY}")).json()[0]
    city_weather = (requests.get(f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat_lon['lat']}&lon={lat_lon['lon']}&date={weather_date}&appid={API_KEY}&units=metric")).json()
    return city_weather

def transform(city_info, city_weather):
    return {
        "place" : city_info[0],
        "city": city_info[1],
        "country": city_info[2],
        "country_code": city_info[3],
        "night": city_weather["temperature"]["night"],
        "afternoon": city_weather["temperature"]["afternoon"],
        "date": city_weather["date"],
    }
    
def load(data):
    conn = sqlite3.connect("./data/weatherData.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            place INT,
            city TEXT,
            country TEXT,
            country_code TEXT,
            night REAL,
            afternoon REAL,
            date DATE,
            PRIMARY KEY (city, date)
        )
    ''')
    cursor.execute('''
        INSERT INTO weather_data (place, city, country, country_code, night, afternoon, date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (data["place"], data["city"], data["country"], data["country_code"], data["night"], data["afternoon"], data["date"]))
    conn.commit()
    conn.close()
    
def weather_pipeline(required_dates):
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    conn = sqlite3.connect("./data/cities.db")
    city_data = pd.read_sql_query("SELECT * FROM city_data", conn)
    conn.close()
    conn = sqlite3.connect("./data/weatherData.db")
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS weather_data
    ''')
    conn.commit()
    conn.close()
    for city in city_data.itertuples(index=False):
        for one_date in required_dates:
            city_weather = extract(city, one_date, API_KEY)
            transformed_data = transform(city, city_weather)
            load(transformed_data)
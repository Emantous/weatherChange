from dotenv import load_dotenv
import os, sqlite3, requests
from datetime import date
from scripts.web_scraping import join_and_get

def extract(city, weather_date, API_KEY):
    lat_lon = (requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city[2]},{city[3]}&appid={API_KEY}")).json()[0]
    city_weather = (requests.get(f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat_lon['lat']}&lon={lat_lon['lon']}&date={weather_date}&appid={API_KEY}&units=metric")).json()
    return city_weather

def transform(city_info, city_weather):
    return {
        "place" : city_info[1],
        "city": city_info[2],
        "country": city_info[0],
        "country_code": city_info[3],
        "night": city_weather["temperature"]["night"],
        "afternoon": city_weather["temperature"]["afternoon"],
        "date": city_weather["date"],
    }
    
def load(data):
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    hook.run('''
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
    hook.run('''
        INSERT INTO weather_data (place, city, country, country_code, night, afternoon, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', parameters=(data["place"], data["city"], data["country"], data["country_code"], data["night"], data["afternoon"], data["date"]))
    
def weather_pipeline():
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    history = [0, 5, 15, 25, 45]
    today = date.today()
    joined = join_and_get()
    for city in joined.itertuples(index=False):
        for years_back in history:
            weather_date = (today.replace(year=today.year - years_back)).isoformat()
            city_weather = extract(city, weather_date, API_KEY)
            transformed_data = transform(city, city_weather)
            load(transformed_data)
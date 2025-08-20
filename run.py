from datetime import datetime, timedelta

from scripts.ETL import weather_pipeline
from scripts.web_scraping import get_codes, find_cities

while True:
    number_of_cities = input("Input the number of cities for data extractions (up to 81):\n")
    try:
        number_of_cities = int(number_of_cities)
        if number_of_cities < 1 or number_of_cities > 81:
            print("Wrong number of cities")
            continue
    except:
        print("The input isn't a number")
        continue
    break

get_codes()
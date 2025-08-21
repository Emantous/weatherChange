from datetime import datetime, timedelta, date

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
find_cities(number_of_cities)

required_dates = []
while True:
    input_date = input("""\nInput one date of desired weather data\nstarting from 1979-01-02 up to the 1,5 years from now\n(in format YYYY-MM-DD)\nor input "X" to end session:\n""")
    if input_date == "X":
        if required_dates == []:
            print("\nNo given dates")
            continue
        else:
            break
    try:
        input_date = datetime.strptime(input_date, "%Y-%m-%d").date()
    except ValueError:
        print("\nWrong date format")
        continue

    min_date = datetime(1979, 1, 2).date()
    max_date = (datetime.now() + timedelta(days=int(365 * 1.5))).date()  # ~1.5 years ahead

    if not (min_date <= input_date <= max_date):
        print("\nThe date is not in the given time range")
        continue
    required_dates.append(input_date)

weather_pipeline(required_dates)
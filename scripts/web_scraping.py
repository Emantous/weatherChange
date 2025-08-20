import requests, sqlite3
from bs4 import BeautifulSoup
import pandas as pd

def find_cities():
    url = "https://en.wikipedia.org/wiki/List_of_largest_cities"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('tbody')[1]
    hook = PostgresHook(postgres_conn_id='postgres_localhost')

    hook.run('''
        DROP TABLE IF EXISTS top_cities
    ''')
    hook.run('''
        CREATE TABLE top_cities (
            place INT,
            name TEXT,
            country TEXT,
            PRIMARY KEY (name)
        )
    ''')
    for place in range(2, 12):
        city = table.find_all('tr')[place].find('th').find('a').contents[0]
        country = table.find_all('tr')[place].find('td').contents[0]
        if city == 'Kinshasa':
            country = 'Congo (the Democratic Republic of the) '
        country = country[:-1]
        if country == 'United States':
            country = 'United States of America (the)'
        elif country == 'Iran':
            country = 'Iran (Islamic Republic of)'
        elif country == 'Turkey':
            country = 'Türkiye'
        elif country == 'Philippines':
            country = 'Philippines (the)'
        elif country == 'Russia':
            country = 'Russian Federation (the)'
        elif country == 'South Korea':
            country = 'Korea (the Republic of)'
        elif country == 'United Kingdom':
            country = 'United Kingdom of Great Britain and Northern Ireland (the)'
        elif country == 'Vietnam':
            country = 'Viet Nam'
        elif country == 'Tanzania':
            country = 'Tanzania, the United Republic of'
        elif country == 'Sudan':
            country = 'Sudan (the)'
        hook.run('''
            INSERT INTO top_cities (
                place,
                name,
                country
            ) VALUES (%s, %s, %s)
        ''', parameters=(place-1, city, country))

def get_codes():
    url = "https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('tbody')[0]
    countries = table.find_all('tr')

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    hook.run('''
        DROP TABLE IF EXISTS country_codes
    ''')
    hook.run('''
        CREATE TABLE country_codes (
            country TEXT,
            code TEXT,
            PRIMARY KEY (country)
        )
    ''')
    for i in range(2, len(countries)):
        code = countries[i].find('span',{ 'class' : 'monospaced'})
        country = countries[i].find('a').contents[0]
        if code is not None:
            if code.contents[0] == 'TW':
                country = 'Taiwan'
            hook.run('INSERT INTO country_codes (country, code) VALUES (%s, %s)', parameters=(country, code.contents[0]))

def join_and_get():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    query = """
            SELECT *
            FROM top_cities
            NATURAL JOIN country_codes
            """
    joined = pd.read_sql_query(query, conn)
    conn.commit()
    conn.close()
    return joined
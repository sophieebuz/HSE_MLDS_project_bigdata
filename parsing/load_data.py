from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import pandas as pd
import time


def parsing_data(url):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    # задержка нужна для прогрузки страницы
    time.sleep(2)
    tree = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    return tree


def get_performances(tree):
    cols = ['date', 'day_of_week', 'type', 'name', 'age', 'time', 'scene', 'tickets', 'price']
    performances = {col: [] for col in cols}

    try:
        posters = tree.find_all('div', {'class': 'PosterCatalogItem'})
        for poster in posters:
            date = poster.find('div', {'class': 'PosterCatalogItem-date'})
            day_of_week = poster.find('div', {'class': 'PosterCatalogItem-day'})
            events = poster.find_all('div', {'class': 'PosterCatalogItem-card'})

            for event in events:
                type = event.find('div', {'class': 'PosterCard-subtitle'})
                name = event.find('div', {'class': 'PosterCard-contentWrapper'})
                age = event.find('div', {'class': 'PosterCard-age'})
                time = event.find('div', {'class': 'PosterCard-time'})
                scene = event.find('div', {'class': 'PosterCard-sceneText'})

                tickets = event.find('div', {'class': 'PosterCard-tickets'})\
                      or event.find('button', {'class': 'RoundedButton PosterCard-button isDisabled'})\
                      or event.find('button', {'class': 'RoundedButton PosterCard-button isTransparentBlack'})
  
                price = event.find('div', {'class': 'PosterCard-price'})

                performances['date'].append(None if date is None else date.text)
                performances['day_of_week'].append(None if day_of_week is None else day_of_week.text)
                performances['type'].append(None if type is None else type.text)
                performances['name'].append(None if name is None else name.text)
                performances['age'].append(None if age is None else age.text)
                performances['time'].append(None if time is None else time.text)
                performances['scene'].append(None if scene is None else scene.text)
                performances['tickets'].append(None if tickets is None else tickets.text)
                performances['price'].append(None if price is None else price.text)
    except:
        print('Error parsing')

    return pd.DataFrame(performances)


def create_performances_parquet(pd_df_performances, parquet_path):
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Bolshoi_Theatre')\
        .getOrCreate()

    df_performances = spark.createDataFrame(pd_df_performances)

    df_performances\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .parquet(parquet_path)


# не забыть удалить
def mock_load_performances(url, parquet_path):
    mock_before = {
        'date': ['12 января', '13 января', '13 января', '14 января', '14 января'],\
        'day_of_week': ['суббота', 'суббота', 'суббота', 'воскресенье', 'воскресенье'],\
        'type': ['Концерт', 'Концерт', 'Балет', 'Балет', 'Балет'],\
        'name': ['Щелкунчик', 'Щелкунчик', 'Чайка', 'Чайка', 'Чайка'],\
        'age': ['12+', '12+', '16+', '16+', '16+'],\
        'time': ['19:00', '19:00', '12:00', '12:00', '13:00'],\
        'scene': ['Историческая сцена', 'Историческая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена'],\
        'tickets': ['Билетов нет', 'Билетов нет', 'Билетов нет', '175 билетов', '10 билетов'],\
        'price': [None, None, None, 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽']
    }

    mock_after = {
        'date': ['13 января', '13 января', '13 января', '14 января', '14 января', '15 января', '16 января'],\
        'day_of_week': ['суббота', 'суббота', 'суббота', 'воскресенье', 'воскресенье', 'понедельник', 'вторник'],\
        'type': ['Концерт', 'Балет', 'Балет', 'Балет', 'Балет', 'Балет', 'Концерт'],\
        'name': ['Щелкунчик', 'Чайка', 'Чайка', 'Чайка', 'Чайка', 'Чайка', 'Щелкунчик'],\
        'age': ['12+', '16+', '16+', '16+', '16+', '16+', '12+'],\
        'time': ['19:00', '12:00', '16:00', '12:00', '13:00', '12:00', '19:00'],\
        'scene': ['Историческая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена', 'Историческая сцена'],\
        'tickets': ['Билетов нет', '177 билетов', '177 билетов', '10 билетов', '5 билетов', '177 билетов', '200 билетов'],\
        'price': [None, 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽']
    }

    if url == 'mock_before':
        pd_df_performances = pd.DataFrame(mock_before) 
    else:
        pd_df_performances = pd.DataFrame(mock_after)

    create_performances_parquet(pd_df_performances, parquet_path)


def load_performances(url, parquet_path):
    tree = parsing_data(url)
    pd_df_performances = get_performances(tree)
    create_performances_parquet(pd_df_performances, parquet_path)

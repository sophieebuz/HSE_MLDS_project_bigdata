import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime 

from selenium import webdriver
from selenium.webdriver.common.by import By

from pyspark.sql import SparkSession


def get_performance(var_names, var_value):
    for i in range(len(var_value)):
        try:
            var_value[i] = var_value[i][0].text
        except:
            var_value[i] = var_value[i]
    
    performance = {}
    for i in range(len(var_names)):
        performance[var_names[i]] = var_value[i]
    
    return performance

def collect_data(tree):
    dict_all = {}
    var_names = ['date', 'day_of_week', 'performance_type', 'performance_name', 'performance_age',
                 'performance_time', 'performance_scene', 'performance_tickets', 'performance_price']
    for i in range(len(var_names)):
        dict_all[var_names[i]] = []
    
    try:
        containers = tree.find_all('div', {'class': 'PosterCatalogItem'})
        for i in range(len(containers)):
            date = containers[i].find_all('div', {'class': 'PosterCatalogItem-date'})
            day_of_week = containers[i].find_all('div', {'class': 'PosterCatalogItem-day'})
            perfs = containers[i].find_all('div', {'class': 'PosterCatalogItem-card'})
            for perf in perfs:
                performance_type = perf.find_all('div', {'class': 'PosterCard-subtitle'})
                performance_name = perf.find_all('div', {'class': 'PosterCard-contentWrapper'})
                performance_age = perf.find_all('div', {'class': 'PosterCard-age'})
                performance_time = perf.find_all('div', {'class': 'PosterCard-time'})
                performance_scene = perf.find_all('div', {'class': 'PosterCard-sceneText'})
                performance_tickets = perf.find_all('div', {'class': 'PosterCard-tickets'})
                performance_price = perf.find_all('div', {'class': 'PosterCard-price'})
                if len(performance_tickets) == 0:
                    performance_tickets = perf.find_all('button', {'class': 'RoundedButton PosterCard-button isDisabled'})
                    performance_price = perf.find_all('button', {'class': 'RoundedButton PosterCard-button isDisabled'})
                    if len(performance_tickets) == 0:
                        performance_tickets = perf.find_all('button', {'class': 'RoundedButton PosterCard-button isTransparentBlack'})
                        performance_price = perf.find_all('button', {'class': 'RoundedButton PosterCard-button isTransparentBlack'})
                if len(performance_age) == 0:
                    performance_age = 'Не указан'
                        
                
                var_value = [date, day_of_week, performance_type, performance_name, performance_age,
                             performance_time, performance_scene, performance_tickets, performance_price]
                
                performance = get_performance(var_names, var_value)

        
                
                for key in dict_all.keys():
                    dict_all[key].append(performance[key])
                
        
        return dict_all
            
    except:
        print('вылетело')
        return dict_all  
 

def get_data():
    br = webdriver.Chrome()
    br.get("https://bolshoi.ru/timetable/all")
    time.sleep(2)

    page_add = br.page_source
    tree = BeautifulSoup(page_add, 'html')

    br.close()
    
    dict_all = collect_data(tree)
    
    df = pd.DataFrame(dict_all)
    df = df.astype("string")
    
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('BigData_project')\
        .getOrCreate()

    df_spark = spark.createDataFrame(df)
    now = datetime.now()
    time_parse = now.strftime("%Y-%m-%d-%H-%M-%S")
    df_spark.repartition(1).write.mode('overwrite').parquet('data/' + time_parse)
    

if __name__ == "__main__":
    get_data()
 

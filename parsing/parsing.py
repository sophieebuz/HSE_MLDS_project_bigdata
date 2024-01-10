from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd


def parsing_performances(tree):
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

    return performances


chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=chrome_options)
url = "https://bolshoi.ru/timetable/all"
driver.get(url)
tree = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()
performances = parsing_performances(tree)
print(pd.DataFrame(performances)[-10:])

import os
import pandas as pd
import pytrends.dailydata
import pytrends.request
from bs4 import BeautifulSoup
import requests

def _get_proxies():
        """
        Ghetto way to get proxies from the free-proxy-list.net site where it grabs the IP and Port and appends them to a list as "IP:Port"
        """
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        ip_table = soup.find('table', attrs={'id': 'proxylisttable'})
        ip_body = ip_table.find('tbody')
        ips = ip_body.findAll('td')

        proxies = []
        for i, _ in enumerate(ips):
            if i % 8 == 0: #1st and 2nd column in row have the IP and Port
                proxy = "https://{}:{}".format(ips[i].text, ips[i+1].text)
                proxies.append(proxy)
        return proxies

def get_release_month_and_year_per_shoe_model_as_dict(df):
    min_year = df.groupby(["model"])["release_year"].min().reset_index()
    min_month = df.groupby(["model"])["release_month"].min().reset_index()
    min_df = min_year.merge(min_month, on='model')
    min_df = min_df.fillna(0)
    return min_df.to_dict()

def get_evaluated_shoe_models():
    path = "shoe_data/shoe_trends/{}/".format(brand)
    return [i for i in os.listdir(path)]

if __name__ == '__main__':
    pytrend = pytrends.request.TrendReq(retries=5, timeout=(100,100), proxies=_get_proxies())
    brand = "adidas"  
    info_df = pd.read_csv("shoe_data/{}_shoe_info.csv".format(brand))
    shoe_models_df = info_df[["model", "release_year", "release_month"]]
    
    min_dict = get_release_month_and_year_per_shoe_model_as_dict(shoe_models_df)
    evaluated_shoe_models = get_evaluated_shoe_models()

    for index in min_dict['model']:
        model = min_dict['model'][index]
        release_year = int(min_dict['release_year'][index])
        release_month = int(min_dict['release_month'][index])
        keyword = brand + " " + model
        file_name = model.replace(" ", "-") + ".csv"

        if file_name not in evaluated_shoe_models:
            if release_year != 0 and release_month != 0:
                daily_data_df = pytrends.dailydata.get_daily_data(word=keyword, start_year=release_year, start_mon=release_month, stop_year=2019, stop_mon=12, pytrends=pytrend)
                daily_data_df = daily_data_df.reset_index()
                
                daily_data_df.to_csv("shoe_data/shoe_trends/{}/{}".format(brand, file_name))
            else:
                print(model + " has no listed release date")
        else:
            print(model + " has already been evaluated")

    
import requests
import csv
import json
from bs4 import BeautifulSoup
from time import sleep
import os
from multiprocessing import Pool

class StockXScraper:
    def __init__(self, brand, pages):
        self.brand = brand
        self.pages = pages
        self.proxies = self._get_proxies()
        self.headers = {
                'User-Agent': "PostmanRuntime/7.19.0",
                'Accept': "*/*",
                'Cache-Control': "no-cache",
                'Postman-Token': "fb0f32b1-b5fa-40de-9e61-a8a392de0c9b,9c3f0fe7-0ea2-4ac7-9645-64b90eedc135",
                'Host': "stockx.com",
                'Accept-Encoding': "gzip, deflate",
                'Cookie': "__cfduid=dcc6263fa54664ce1a47c2cd9ce4244a01572311165",
                'Connection': "keep-alive",
                'cache-control': "no-cache"
            }

    def get_shoe_links(self, page):
        """
        Takes in a shoe brand (nike or adidas) and amount of pages (25 per page) and creates a list of all the links
        and stores in a csv for in the shoe_links directory
        """
        MAX_PAGES = 25
        shoe_links= []
        if page > MAX_PAGES:
            return

        url = "https://stockx.com/{}?page={}".format(self.brand, page)
        print("StockX Page Number: {}".format(page))
        for i in range(10):
            try:
                proxy = self.proxies[i % len(self.proxies)]
                response = requests.request("GET", url, headers=self.headers, proxies={'https': proxy, 'http': proxy})
                print("Proxy Value: {}, Request Status Code: {}".format(proxy, response.status_code))
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml')
                    products_div = soup.find('div', attrs={'class': 'browse-grid'})
                    products = products_div.findAll('a', href=True)
                    for link in products:
                        if link['href'] not in products:
                            shoe_links.append(link['href'])
                    break
            except:
                #sleep(10)
                continue

        return shoe_links

    def get_shoe_info(self, link):
        """
        Takes in a csv file containing links to different shoes and gets a json file containing information about the shoe
        such as name, brand, release date, colorway, and sku and stores it as a JSON file in the shoe_info directory writes
        SKUs to csv in shoe_transactions directory
        """
        url = "https://stockx.com{}".format(link)
        print("Extracting shoe info from {}".format(url))
        for i in range(10):
            try:
                proxy = self.proxies[i % len(self.proxies)]
                response = requests.request("GET", url, headers=self.headers, proxies={'https': proxy, 'http': proxy})
                print("Proxy Value: {}, Request Status Code: {}".format(proxy, response.status_code))
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml')
                    scripts = soup.findAll('script', type='application/ld+json')

                    #gets the last script which has shoe info
                    info = scripts[-1].text
                    #removes script tags to turn have only the JSON object
                    info = info.replace("</script>", "")
                    info = info.replace('<script type="application/ld+json">', "")

                    data = json.loads(info)
                    file_name = "shoe_info/{}/{}.json".format(self.brand, data["sku"])
                    self._write_to_json(file_name, data)

                    print("JSON file: {} created".format(file_name))
                    break
            except:
                #sleep(10)
                continue

    def get_shoe_transaction_data(self, sku):
        """
        reads each SKU from the shoe_info directory and for each SKU grabs up 100,000 transactions with information about date of transaction, size, and price and stores it as a JSON file
        in the shoe_transactions directory
        """
        if self._check_if_shoe_transaction_already_scraped(sku):
            print("JSON file: {} already exists, skipping")
            return
        url = "https://stockx.com/api/products/{}/activity".format(sku)
        querystring = {"state":"480","currency":"USD","limit":"100000","page":"1","sort":"createdAt","order":"DESC"}
        for i in range(10):
            try:
                proxy = self.proxies[i % len(self.proxies)]
                response = requests.request("GET", url, headers=self.headers, params=querystring, proxies={'https': proxy, 'http': proxy})
                print("Proxy Value: {}, Request Status Code: {}, SKU Value: {}".format(proxy, response.status_code, sku))
                if response.status_code == 200:
                    data = response.json()
                    file_name = 'shoe_transactions/{}/{}.json'.format(self.brand, sku)
                    print("JSON file: {} created".format(file_name))
                    self._write_to_json(file_name, data)

            except:
                continue

    def _get_proxies(self):
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
                proxy = "{}:{}".format(ips[i].text, ips[i+1].text)
                proxies.append(proxy)
        return proxies

    def _write_to_csv(self, file_name, data_list):
        with open(file_name, 'w') as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(data_list)

    def _write_to_json(self, file_name, data):
        with open(file_name, 'w') as f:
            json.dump(data, f)

    def _get_list_from_csv(self, file_name):
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            unflattened_list = list(reader)
    
        flattened_list = []
        for inner_list in unflattened_list:
            for value in inner_list:
                flattened_list.append(value)
        return flattened_list

    def _get_sku_list(self, data_type):
        directory_name = "shoe_{}/{}/".format(data_type, self.brand)
        files = os.listdir(directory_name)
        sku_list = []
        for file in files:
            sku_list.append(file.replace(".json", ""))
        return sku_list

    def _check_if_shoe_link_already_scraped(self, shoe_link, shoe_links):
        if shoe_link in shoe_links:
            return True
        return False

    def _check_if_shoe_info_already_scraped(self, sku, sku_list):
        if sku in sku_list:
            return True
        return False

    def _check_if_shoe_transaction_already_scraped(self, sku):
        sku_list = self._get_sku_list("transactions")
        if sku in sku_list:
            return True
        return False

    

if __name__ == '__main__':
    stock_x_scraper = StockXScraper("adidas", 20)
    
    print("Getting Shoe Links:")
    pages = range(20, 25)
    pool = Pool(processes=8)
    links_result = pool.map(stock_x_scraper.get_shoe_links, pages)
    pool.close()
    pool.join()
    print(links_result)


    print("Getting Shoe Info:")
    links = stock_x_scraper._get_list_from_csv("shoe_links/adidas_links.csv")
    start_index = 333
    pool = Pool(processes=8)
    info_result = pool.map(stock_x_scraper.get_shoe_info, links[start_index:])
    pool.close()
    pool.join()
    

    print("Getting Shoe Transaction Data:")
    sku_list = stock_x_scraper._get_sku_list("info")
    pool = Pool(processes=8)
    transaction_result = pool.map(stock_x_scraper.get_shoe_transaction_data, sku_list)
    pool.close()
    pool.join()
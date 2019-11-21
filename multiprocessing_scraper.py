import requests
import csv
import json
from bs4 import BeautifulSoup
from random import randint
import os
from multiprocessing import Pool, Process, cpu_count, current_process

class StockXScraper:
    def __init__(self, brand):
        self.brand = brand
        self.proxies = self._get_proxies()
        self.scraped_pages = self._get_scraped_pages()
        self.scraped_shoe_info_links = self._get_scraped_shoe_info_links()
        self.scraped_sku_list = self._get_sku_list("transactions")
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
        Takes in a shoe brand (nike or adidas) and amount of pages up to 25 pages total and creates a list of all the links
        and stores in a csv in the shoe_links directory
        """
        shoe_links = []
        url = "https://stockx.com/{}?page={}".format(self.brand, page)
        #print("StockX Page Number: {}".format(page))
        success = False
        for _ in range(10):
            try:
                proxy = self.proxies[randint(0, len(self.proxies))]
                response = requests.request("GET", url, headers=self.headers, proxies={'https': proxy, 'http': proxy})
                #print("Proxy Value: {}, Request Status Code: {}".format(proxy, response.status_code))
                if response.status_code == 200:
                    shoe_links = self._scrape_shoe_links(response.text)
                    file_name = "shoe_links/{}/{}.csv".format(self.brand, page)
                    self._write_to_csv(file_name, shoe_links)
                    print("Shoe links scraped from page {}, Process ID: {}".format(page, current_process().pid))
                    success = True
                    break
            except:
                continue
        if success == False:
            print("Unable to get information from {}, Process ID: {}".format(url, current_process().pid))

    def get_shoe_info(self, link):
        """
        Takes in a csv file containing links to different shoes and gets a json file containing information about the shoe
        such as name, brand, release date, colorway, and sku and stores it as a JSON file in the shoe_info directory writes
        SKUs to csv in shoe_transactions directory
        """
        url = "https://stockx.com{}".format(link)
        if self._check_if_shoe_info_already_scraped(url):
            #print("Already exists: JSON file for {}".format(url))
            return

        print("Extracting shoe info from {}".format(url))
        success = False
        for _ in range(10):
            try:
                proxy = self.proxies[randint(0, len(self.proxies))]
                response = requests.request("GET", url, headers=self.headers, proxies={'https': proxy, 'http': proxy})
                #print("Proxy Value: {}, Request Status Code: {}, Shoe Link: {}".format(proxy, response.status_code, link))
                if response.status_code == 200:
                    shoe_info = self._scrape_shoe_info(response.text)
                    file_name = "shoe_info/{}/{}.json".format(self.brand, shoe_info["sku"])
                    self._write_to_json(file_name, shoe_info)
                    print("JSON file: {} created, Process ID: {}".format(file_name, current_process().pid))
                    success = True
                    break
            except:
                continue
        if success == False:
            print("Unable to get information from {}, Process ID: {}".format(url, current_process().pid))
        
    def get_shoe_transaction_data(self, sku):
        """
        reads each SKU from the shoe_info directory and for each SKU grabs up 100,000 transactions with information about date of transaction, size, and price and stores it as a JSON file
        in the shoe_transactions directory
        """
        if self._check_if_shoe_transaction_already_scraped(sku):
            #print("Already exists: JSON file for {}".format(sku))
            return

        url = "https://stockx.com/api/products/{}/activity".format(sku)
        querystring = {"state":"480","currency":"USD","limit":"100000","page":"1","sort":"createdAt","order":"DESC"}

        print("Extracting shoe info from {}".format(url))
        success = False
        for _ in range(10):
            try:
                proxy = self.proxies[randint(0, len(self.proxies))]
                response = requests.request("GET", url, headers=self.headers, params=querystring, proxies={'https': proxy, 'http': proxy})
                #print("Proxy Value: {}, Request Status Code: {}, SKU Value: {}, Process ID: {}".format(proxy, response.status_code, sku, current_process().pid))
                if response.status_code == 200:
                    data = response.json()
                    file_name = 'shoe_transactions/{}/{}.json'.format(self.brand, sku)
                    self._write_to_json(file_name, data)
                    print("JSON file: {} created, Process ID: {}".format(file_name, current_process().pid))
                    success = True
                    break
            except:
                continue
        if success == False:
            print("Unable to get information from {}, Process ID: {}".format(url, current_process().pid))

    def join_shoe_links_csv_files(self):
        directory_name = "shoe_links/{}/".format(self.brand)
        file_names = os.listdir(directory_name)
        file_contents = []
        for file_name in file_names:
            file_contents.append(self._get_list_from_csv(directory_name + file_name))
        file_contents_flattened = self._flatten_list(file_contents)
        output_path = "shoe_links/{}_links.csv".format(self.brand.replace("-", "_"))
        self._write_to_csv(output_path, file_contents_flattened)

    def _scrape_shoe_links(self, text):
        soup = BeautifulSoup(text, 'lxml')
        products_div = soup.find('div', attrs={'class': 'browse-grid'})
        products = products_div.findAll('a', href=True)
        shoe_links = []
        for link in products:
            if link['href'] not in products:
                shoe_links.append(link['href'])
        return shoe_links

    def _scrape_shoe_info(self, text):
        soup = BeautifulSoup(text, 'lxml')
        page_scripts = soup.findAll('script', type='application/ld+json')
        #gets the last script which has shoe info
        raw_shoe_info = page_scripts[-1].text
        #removes script tags to have only the JSON object
        raw_shoe_info = raw_shoe_info.replace("</script>", "")
        json_formatted_shoe_info = raw_shoe_info.replace('<script type="application/ld+json">', "")
        shoe_data = json.loads(json_formatted_shoe_info, strict=False)
        return shoe_data

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

    def _get_scraped_pages(self):
        directory_name = "shoe_links/{}/".format(self.brand)
        file_names = os.listdir(directory_name)
        return [file_name.replace(".csv", "") for file_name in file_names]

    def _get_scraped_shoe_info_links(self):
        directory_name = "shoe_info/{}/".format(self.brand)
        file_names = os.listdir(directory_name)
        scraped_links = []
        for file_name in file_names:
            file_path = directory_name + file_name
            with open(file_path) as json_file:
                data = json.load(json_file)
                try:
                    scraped_link = data["offers"]["url"]
                    scraped_links.append(scraped_link)
                except KeyError:
                    continue
        return scraped_links

    def _get_list_from_csv(self, file_name):
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            unflattened_list = list(reader)
    
        return self._flatten_list(unflattened_list)

    def _get_sku_list(self, data_type):
        directory_name = "shoe_{}/{}/".format(data_type, self.brand)
        file_names = os.listdir(directory_name)
        sku_list = []
        for file_name in file_names:
            sku_list.append(file_name.replace(".json", ""))
        return sku_list

    def _write_to_csv(self, file_name, data_list):
        with open(file_name, 'w') as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(data_list)

    def _write_to_json(self, file_name, data):
        with open(file_name, 'w') as f:
            json.dump(data, f)

    def _check_if_shoe_info_already_scraped(self, shoe_link):
        for scraped_link in self.scraped_shoe_info_links:
            if scraped_link == shoe_link:
                return True
        return False

    def _check_if_shoe_transaction_already_scraped(self, sku):
        if sku in self.scraped_sku_list:
            return True
        return False

    def _flatten_list(self, unflattened_list): 
        flattened_list = []
        for inner_list in unflattened_list:
            if isinstance(inner_list, list):
                for value in inner_list:
                    flattened_list.append(value)
            else:
                continue
        return flattened_list

def scrape_shoe_links(stock_x_scraper):
    pages = range(1, 26)
    pages = [str(page) for page in pages]
    pages_to_scrape = [page for page in pages if page not in stock_x_scraper.scraped_pages]
    print("Amount of pages to scrape: {}".format(len(pages_to_scrape)))
    pool = Pool(processes=cpu_count())
    pool.map_async(stock_x_scraper.get_shoe_links, pages_to_scrape)
    pool.close()
    pool.join()
    stock_x_scraper.join_shoe_links_csv_files()

def scrape_shoe_info(stock_x_scraper):
    links = stock_x_scraper._get_list_from_csv("shoe_links/{}_links.csv".format(stock_x_scraper.brand.replace("-", "_")))
    links_to_scrape = [link for link in links if "https://stockx.com" + link not in stock_x_scraper.scraped_shoe_info_links]
    print("Amount of links to scrape: {}".format(len(links_to_scrape)))
    pool = Pool(processes=50)
    pool.map(stock_x_scraper.get_shoe_info, links_to_scrape)
    pool.close()
    pool.join()

def scrape_transaction_data(stock_x_scraper):
    sku_list = stock_x_scraper._get_sku_list("info")
    skus_to_scrape = [sku for sku in sku_list if sku not in stock_x_scraper.scraped_sku_list]
    print("Amount of SKUs to scrape: {}".format(len(skus_to_scrape)))
    pool = Pool(processes=50)
    pool.map_async(stock_x_scraper.get_shoe_transaction_data, skus_to_scrape)
    pool.close()
    pool.join()
    
if __name__ == '__main__':
    #adidas, nike, retro-jordans, other-sneakers
    brand = "other-sneakers"
    stock_x_scraper = StockXScraper(brand)

    #print("Getting Shoe Links:")
    #scrape_shoe_links(stock_x_scraper)
    
    #print("Getting Shoe Info:")
    #scrape_shoe_info(stock_x_scraper)

    #print("Getting Shoe Transaction Data:")
    #scrape_transaction_data(stock_x_scraper)
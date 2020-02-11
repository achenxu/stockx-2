import json
import csv
import os

class StockXDataFormatter:
    def __init__(self, brand):
        self.brand = brand
        self.shoe_info_keys = ["link_name", "sku", "name", "brand", "model", "color", "releaseDate"]
        self.shoe_transaction_keys = ["sku", "link_name", "amount", "createdAt", "shoeSize", "localCurrency"]

    def create_shoe_info_csv(self):
        sku_list = self._get_sku_list()

        rows = [self.shoe_info_keys] #starts with self.shoe_info_keys as header
        for sku in sku_list:
            file_name = "shoe_info/" + self.brand + "/" + sku + ".json"
            values = self._filter_shoe_info_keys(file_name, self.shoe_info_keys)
            rows.append(values)
        rows = [row for row in rows if row != []] #remove empty rows 
        file_name = "shoe_data/{}_shoe_info.csv".format(self.brand.replace("-", "_"))
        self._write_to_csv(file_name, rows)
        print("CSV file {} created".format(file_name))
        
    def create_shoe_transaction_csvs(self):
        sku_list = self._get_sku_list()

        header = self.shoe_transaction_keys #starts with filter_list as header
        for sku in sku_list:
            try:
                link_name = self._get_shoe_link_name(sku)
                file_name = "shoe_transactions/" + self.brand + "/" + sku + ".json"
                rows = self._filter_shoe_transactions_keys(header, file_name, sku, link_name, self.shoe_transaction_keys)
                rows = [row for row in rows if row != []] #remove empty rows 
                output_file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, link_name)
                self._write_to_csv(output_file_name, rows)
            except FileNotFoundError:
                print("{} does not exist".format(file_name))
                continue
        print("CSV files created in shoe_data/shoe_transactions/{}/ directory".format(self.brand))

    def _get_shoe_link_name(self, sku):
        file_name = "shoe_data/{}_shoe_info.csv".format(self.brand.replace("-", "_"))
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            file_content = list(reader)

        for row in file_content:
            if row[1] == sku:
                return row[0]

    def _get_list_from_csv(self, file_name):
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            unflattened_list = list(reader)
    
        flattened_list = []
        for inner_list in unflattened_list:
            for value in inner_list:
                flattened_list.append(value)
        return flattened_list

    def _get_sku_list(self):
        directory_name = "shoe_info/{}/".format(self.brand)
        files = os.listdir(directory_name)
        sku_list = []
        for file in files:
            sku_list.append(file.replace(".json", ""))
        return sku_list

    def _write_to_csv(self, file_name, data):
        with open(file_name, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def _filter_shoe_info_keys(self, json_file, filter_list):
        with open(json_file) as f:
            data = json.load(f)
        try:
            shoe_link = data["offers"]["url"].replace("https://stockx.com/", "")
            values = [shoe_link]
            for key in filter_list:
                if key in data.keys():
                    values.append(data[key])
            
            return values
        except:
            return []

    def _filter_shoe_transactions_keys(self, header, json_file, sku, link_name, filter_list):
        with open(json_file) as f:
            data = json.load(f)

        values = [header]
        try:
            for transaction in data["ProductActivity"]:
                transaction_values = [sku, link_name]
                for filter_key in filter_list:
                    if filter_key in transaction.keys():
                        transaction_values.append(transaction[filter_key])
                values.append(transaction_values)
            return values
        except KeyError: #some shoes don't have any transaction data
            return []
    

if __name__ == '__main__':
    #adidas, nike, retro-jordans, other-sneakers
    formatter = StockXDataFormatter("adidas")

    print("Formatting scraped data:")
    formatter.create_shoe_info_csv()
    formatter.create_shoe_transaction_csvs()
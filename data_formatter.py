import json
import csv
import os

class StockXDataFormatter:
    def __init__(self, brand):
        self.brand = brand
        self.shoe_info_keys = ["name", "brand", "model", "sku", "color", "releaseDate"]
        self.shoe_transaction_keys = ["sku", "amount", "createdAt", "shoeSize", "localCurrency"]

    def create_shoe_info_csv(self):
        """
        Creates csv file containing data for each shoe based on SKU from sku
        """
        sku_list = self._get_sku_list()

        rows = [self.shoe_info_keys] #starts with self.shoe_info_keys as header
        for sku in sku_list:
            file_name = "shoe_info/" + self.brand + "/" + sku + ".json"
            values = self._filter_shoe_info_keys(file_name, self.shoe_info_keys)
            rows.append(values)

        file_name = "shoe_data/{}_shoe_info.csv".format(self.brand)
        self._write_to_csv(file_name, rows)

    def create_shoe_transactions_csv(self):
        """
        Creates csv file containing data for each shoe based on SKU from sku
        """
        sku_list = self._get_sku_list()

        header = self.shoe_transaction_keys #starts with filter_list as header
        for sku in sku_list:
            try:
                file_name = "shoe_transactions/" + self.brand + "/" + sku + ".json"
                rows = self._filter_shoe_transactions_keys(file_name, sku, self.shoe_transaction_keys)
                rows.insert(0, header)

                file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, sku)
                self._write_to_csv(file_name, rows)
            except FileNotFoundError:
                print("{} does not exist".format(file_name))
                continue

    def _write_to_csv(self, file_name, data):
        with open(file_name, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(data)

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

    def _filter_shoe_info_keys(self, json_file, filter_list):
        with open(json_file) as f:
            data = json.load(f)

        values = []
        for key in filter_list:
            if key in data.keys():
                values.append(data[key])
        return values

    def _filter_shoe_transactions_keys(self, json_file, sku, filter_list):
        with open(json_file) as f:
            data = json.load(f)

        values = []
        for transaction in data["ProductActivity"]:
            transaction_values = [sku]
            for filter_key in filter_list:
                if filter_key in transaction.keys():
                    transaction_values.append(transaction[filter_key])
            values.append(transaction_values)
        return values

if __name__ == '__main__':
    formatter = StockXDataFormatter("adidas")

    #formatter.create_shoe_info_csv()
    formatter.create_shoe_transactions_csv()
import json
import csv
import os
from datetime import datetime
import pandas as pd

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

    def _get_shoe_name_list(self):
        directory_name = "shoe_data/shoe_transactions/{}/".format(self.brand)
        files = os.listdir(directory_name)
        shoe_names = []
        for file in files:
            shoe_names.append(file.replace(".csv", ""))
        return shoe_names

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

    def _format_model(self, shoe_info_df):
        if self.brand == "retro-jordans":
            shoe_models = [shoe.replace("Jordan ", "") for shoe in list(shoe_info_df["model"])]
        else:
            shoe_models = [shoe.replace("{} ".format(self.brand), "") for shoe in list(shoe_info_df["model"])]
        shoe_info_df["model"] = shoe_models

    def _format_name(self, shoe_info_df):
        if self.brand == "retro-jordans":
            shoe_names = [shoe.replace("Jordan", "") for shoe in list(shoe_info_df["name"])]
        else:
            shoe_names = [shoe.replace("{} ".format(self.brand), "") for shoe in list(shoe_info_df["name"])]
        shoe_info_df["name"] = shoe_names

    def _add_color_info(self, shoe_info_df):
        colorways = [color.split("/") for color in list(shoe_info_df["color"])]
        most_colors_in_colorway = self._find_max_length(colorways)
        for i in range(most_colors_in_colorway):
            shoe_info_df["color_{}".format(i+1)] = [color[i] if i < len(color) else "N/A" for color in colorways]

    def _add_date_info(self, shoe_info_df):
        date_split = [date_.split("-") for date_ in list(shoe_info_df["releaseDate"])]
        shoe_info_df["release_year"] = [date_[0] if len(date_) == 3 else "N/A" for date_ in date_split]
        shoe_info_df["release_month"] = [date_[1] if len(date_) == 3 else "N/A" for date_ in date_split]
        shoe_info_df["release_day"] = [date_[2] if len(date_) == 3 else "N/A" for date_ in date_split]

    def _find_max_length(self, list_of_lists):
        lens = [len(list_) for list_ in list_of_lists]
        return max(lens)

    def _get_release_date_from_sku(self, sku):
        file_name = "shoe_data/{}_shoe_info.csv".format(self.brand.replace("-", "_"))
        shoe_info_df = pd.read_csv(file_name)
        info = shoe_info_df.loc[shoe_info_df["sku"] == sku, ["releaseDate"]]
        return info["releaseDate"].values[0]

    def add_additional_features(self):
        shoe_info_file_name = "shoe_data/{}_shoe_info.csv".format(self.brand.replace("-", "_"))
        shoe_info_df = pd.read_csv(shoe_info_file_name)
        if self.brand != "other-sneakers":
            self._format_model(shoe_info_df)
            self._format_name(shoe_info_df)
        self._add_color_info(shoe_info_df)
        self._add_date_info(shoe_info_df)
        
        self._add_blank_columns_to_df(shoe_info_df)
        
        shoe_names = self._get_shoe_name_list()
        for shoe in shoe_names:
            try:
                file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, shoe)
                shoe_transaction_df = pd.read_csv(file_name)
                sku = shoe_transaction_df["sku"].values[0]
                shoe_release_date = self._get_release_date_from_sku(sku)
                self._add_release_date_delta(shoe_transaction_df, shoe_release_date)
                self._add_rolling_mean_by_shoe_size(shoe_transaction_df)
                self._get_max_price_and_date_and_days_since_release(sku, shoe_transaction_df, shoe_info_df)
                self._get_min_price_and_date_and_days_since_release(sku, shoe_transaction_df, shoe_info_df)
                self._get_month_and_year_with_most_transactions(sku, shoe_transaction_df, shoe_info_df)
                self._get_most_popular_shoe_size(sku, shoe_transaction_df, shoe_info_df)
                shoe_transaction_df.to_csv(file_name, index=False)
            except:
                continue


        shoe_info_df.to_csv(shoe_info_file_name, index=False)

    def _add_blank_columns_to_df(self, shoe_info_df):
        shoe_info_df["month_and_year_with_most_transactions"] = "N/A"
        shoe_info_df["min_price"] = "N/A"
        shoe_info_df["min_price_date"] = "N/A"
        shoe_info_df["min_price_days_since_release"] = "N/A"
        shoe_info_df["max_price"] = "N/A"
        shoe_info_df["max_price_date"] = "N/A"
        shoe_info_df["max_price_days_since_release"] = "N/A"
        shoe_info_df["most_popular_size"] = "N/A"

    def _add_rolling_mean_by_shoe_size(self, shoe_transaction_df):
        shoe_transaction_df["rolling_average_price_by_size"] = shoe_transaction_df.groupby('shoeSize')['amount'].rolling(5).mean().reset_index(0, drop=True)

    def _add_release_date_delta(self, shoe_transaction_df, shoe_release_date):
        try:
            date_format = "%Y-%m-%d"
            shoe_release_date_formatted = datetime.strptime(shoe_release_date, date_format)
            shoe_transactions_dates = list(pd.to_datetime(shoe_transaction_df["createdAt"]))
            deltas = [transaction_date - shoe_release_date_formatted for transaction_date in shoe_transactions_dates]
            days = [d.days for d in deltas]
            shoe_transaction_df["days_since_release"] = days
            shoe_transaction_df["release_date"] = shoe_release_date_formatted
        except ValueError: #date is either -- or in different format
            shoe_transaction_df["days_since_release"] = "N/A"
            shoe_transaction_df["release_date"] = "N/A"

    def _get_month_and_year_with_most_transactions(self, sku, shoe_transaction_df, shoe_info_df):
        most_popular = shoe_transaction_df['createdAt'].groupby(pd.to_datetime(shoe_transaction_df['createdAt']).dt.to_period("M")).agg('count').idxmax()
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "month_and_year_with_most_transactions"] = most_popular

    def _get_min_price_and_date_and_days_since_release(self, sku, shoe_transaction_df, shoe_info_df):
        min_index = shoe_transaction_df['amount'].idxmin()
        min_amount = shoe_transaction_df['amount'].iloc[min_index]
        min_date = shoe_transaction_df['createdAt'].iloc[min_index]
        min_days_since_release = shoe_transaction_df['days_since_release'].iloc[min_index]

        shoe_info_df.loc[shoe_info_df["sku"] == sku, "min_price"] = min_amount
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "min_price_date"] = min_date
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "min_price_days_since_release"] = min_days_since_release

    def _get_max_price_and_date_and_days_since_release(self, sku, shoe_transaction_df, shoe_info_df):
        max_index = shoe_transaction_df['amount'].idxmax()
        max_amount = shoe_transaction_df['amount'].iloc[max_index]
        max_date = shoe_transaction_df['createdAt'].iloc[max_index]
        max_days_since_release = shoe_transaction_df['days_since_release'].iloc[max_index]

        shoe_info_df.loc[shoe_info_df["sku"] == sku, "max_price"] = max_amount
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "max_price_date"] = max_date
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "max_price_days_since_release"] = max_days_since_release

    def _get_most_popular_shoe_size(self, sku, shoe_transaction_df, shoe_info_df):
        max_index = shoe_transaction_df['amount'].idxmax()
        most_popular_size = shoe_transaction_df['shoeSize'].iloc[max_index]
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "most_popular_size"] = most_popular_size


if __name__ == '__main__':
    #adidas, nike, retro-jordans, other-sneakers
    formatter = StockXDataFormatter("other-sneakers")

    #Formats scraped data
    #formatter.create_shoe_info_csv()
    #formatter.create_shoe_transaction_csvs()

    #Adds more columns and features
    formatter.add_additional_features()
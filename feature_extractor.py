import os
import json
import urllib.request
from urllib.request import urlopen
from datetime import datetime
import pandas as pd
from multiprocessing import Pool, Process, cpu_count, current_process

class StockXFeatureExtractor:
    def __init__(self, brand):
        self.brand = brand
        self.subreddits = ["r/Sneakers", "r/sneakermarket", "r/sneakerhead"]
        self.url = ""

    def _read_json(self, file_name):
        with open(file_name) as json_file:
            data = json.load(json_file)
        return data

    def _get_reddit_json(self):
        url = "https://www.reddit.com/r/learnpython.json?q=timestamp%3A1410739200..1411171200"
        
        with urllib.request.urlopen(self.url) as url:
            subreddit_json = json.loads(url.read().decode())["data"]
        return subreddit_json

    def _add_color_info(self, shoe_info_df):
        colorways = [color.split("/") for color in list(shoe_info_df["color"])]
        most_colors_in_colorway = self._find_max_length(colorways)
        for i in range(most_colors_in_colorway):
            shoe_info_df["color_{}".format(i+1)] = [color[i] if i < len(color) else "N/A" for color in colorways]

    def _format_model_col(self, shoe_info_df):
        if self.brand == "retro-jordans":
            shoe_models = [shoe.replace("Jordan ", "") for shoe in list(shoe_info_df["model"])]
        else:
            shoe_models = [shoe.replace("{} ".format(self.brand), "") for shoe in list(shoe_info_df["model"])]
        shoe_info_df["model"] = shoe_models

    def _format_name_col(self, shoe_info_df):
        if self.brand == "retro-jordans":
            shoe_names = [shoe.replace("Jordan", "") for shoe in list(shoe_info_df["name"])]
        else:
            shoe_names = [shoe.replace("{} ".format(self.brand), "") for shoe in list(shoe_info_df["name"])]
        shoe_info_df["name"] = shoe_names

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

    def _get_shoe_name_list(self):
        directory_name = "shoe_data/shoe_transactions/{}/".format(self.brand)
        files = os.listdir(directory_name)
        shoe_names = []
        for file_name in files:
            shoe_names.append(file_name.replace(".csv", ""))
        return shoe_names

    def _add_blank_columns_to_info_df(self, shoe_info_df):
        shoe_info_df["month_and_year_with_most_transactions"] = "N/A"
        shoe_info_df["min_price"] = "N/A"
        shoe_info_df["min_price_date"] = "N/A"
        shoe_info_df["min_price_days_since_release"] = "N/A"
        shoe_info_df["max_price"] = "N/A"
        shoe_info_df["max_price_date"] = "N/A"
        shoe_info_df["max_price_days_since_release"] = "N/A"
        shoe_info_df["most_popular_size"] = "N/A"

    def _add_rolling_mean(self, shoe_transaction_df):
        shoe_transaction_df["rolling_average_price"] = shoe_transaction_df.sort_values(by=['createdAt'])["amount"].rolling(10).mean()

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

    def _add_colorway_and_model(self, sku, shoe_transaction_df, shoe_info_df):
        colorway = shoe_info_df.loc[shoe_info_df["sku"] == sku, "color"]
        model = shoe_info_df.loc[shoe_info_df["sku"] == sku, "model"]

        shoe_transaction_df["color"] = colorway.values[0]
        shoe_transaction_df["model"] = model.values[0]

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
        most_popular_size = shoe_transaction_df['shoeSize'].value_counts().idxmax()
        shoe_info_df.loc[shoe_info_df["sku"] == sku, "most_popular_size"] = most_popular_size
    
    def _get_average_price_per_day(self, shoe_transaction_df):
        shoe_transaction_df["average_price_per_day"] = shoe_transaction_df['amount'].groupby(pd.to_datetime(shoe_transaction_df['createdAt']).dt.to_period("D")).transform('mean')

    def _nearest_date(self, dates, date):
        return min(dates, key=lambda x: abs(x - date))

    def _get_past_and_future_prices(self, shoe_transaction_df):
        df = shoe_transaction_df
        df["transaction_date"] = pd.to_datetime(df["createdAt"]).dt.date

        df["date_5_days_ago"] = pd.to_datetime(df["transaction_date"] - pd.DateOffset(days=5)).dt.date
        df["date_15_days_ago"] = pd.to_datetime(df["transaction_date"] - pd.DateOffset(days=15)).dt.date
        df["date_30_days_ago"] = pd.to_datetime(df["transaction_date"] - pd.DateOffset(days=30)).dt.date

        df["date_5_days_future"] = pd.to_datetime(df["transaction_date"] + pd.DateOffset(days=5)).dt.date
        df["date_15_days_future"] = pd.to_datetime(df["transaction_date"] + pd.DateOffset(days=15)).dt.date
        df["date_30_days_future"] = pd.to_datetime(df["transaction_date"] + pd.DateOffset(days=30)).dt.date

        p5 = df["date_5_days_ago"].tolist()
        p15 = df["date_15_days_ago"].tolist()
        p30 = df["date_30_days_ago"].tolist()

        f5 = df["date_5_days_future"].tolist()
        f15 = df["date_15_days_future"].tolist()
        f30 = df["date_30_days_future"].tolist()

        p5_amount_list = []
        p15_amount_list = []
        p30_amount_list = []

        f5_amount_list = []
        f15_amount_list = []
        f30_amount_list = []

        transaction_list = df["transaction_date"].tolist()
        for index in range(len(p5)):
            nearest_past_date_5 = self._nearest_date(transaction_list, p5[index])
            nearest_past_date_15 = self._nearest_date(transaction_list, p15[index])
            nearest_past_date_30 = self._nearest_date(transaction_list, p30[index])

            nearest_future_date_5 = self._nearest_date(transaction_list, f5[index])
            nearest_future_date_15 = self._nearest_date(transaction_list, f15[index])
            nearest_future_date_30 = self._nearest_date(transaction_list, f30[index])

            p5_amount = df.loc[df["transaction_date"] == nearest_past_date_5, "amount"].mean()
            p15_amount = df.loc[df["transaction_date"] == nearest_past_date_15, "amount"].mean()
            p30_amount = df.loc[df["transaction_date"] == nearest_past_date_30, "amount"].mean()

            f5_amount = df.loc[df["transaction_date"] == nearest_future_date_5, "amount"].mean()
            f15_amount = df.loc[df["transaction_date"] == nearest_future_date_15, "amount"].mean()
            f30_amount = df.loc[df["transaction_date"] == nearest_future_date_30, "amount"].mean()

            p5_amount_list.append(p5_amount)
            p15_amount_list.append(p15_amount)
            p30_amount_list.append(p30_amount)

            f5_amount_list.append(f5_amount)
            f15_amount_list.append(f15_amount)
            f30_amount_list.append(f30_amount)

        shoe_transaction_df["price_5_days_ago"] = p5_amount_list
        shoe_transaction_df["price_15_days_ago"] = p15_amount_list
        shoe_transaction_df["price_30_days_ago"] = p30_amount_list

        shoe_transaction_df["price_5_days_future"] = f5_amount_list
        shoe_transaction_df["price_15_days_future"] = f15_amount_list
        shoe_transaction_df["price_30_days_future"] = f30_amount_list

    def _add_shoe_info_features(self, shoe_info_df, shoe_names):
        for shoe in shoe_names:
            try:
                file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, shoe)
                shoe_transaction_df = pd.read_csv(file_name)
                sku = shoe_transaction_df["sku"].values[0]
                shoe_release_date = self._get_release_date_from_sku(sku)

                self._add_release_date_delta(shoe_transaction_df, shoe_release_date)
                self._get_max_price_and_date_and_days_since_release(sku, shoe_transaction_df, shoe_info_df)
                self._get_min_price_and_date_and_days_since_release(sku, shoe_transaction_df, shoe_info_df)
                self._get_month_and_year_with_most_transactions(sku, shoe_transaction_df, shoe_info_df)
                self._get_most_popular_shoe_size(sku, shoe_transaction_df, shoe_info_df)
                self._add_colorway_and_model(sku, shoe_transaction_df, shoe_info_df)
            except:
                continue
        
    def _drop_util_columns_from_df(self, shoe):
        try:
            file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, shoe)
            shoe_transaction_df = pd.read_csv(file_name)
            shoe_transaction_df = shoe_transaction_df.drop(columns=["date_5_days_future", "date_15_days_future", "date_30_days_future", "date_5_days_ago", "date_15_days_ago", "date_30_days_ago"])
            shoe_transaction_df.to_csv(file_name, index=False)
        except:
            pass

    def _add_additional_features_transaction_data(self, shoe):
        expected_columns = ['sku', 'link_name', 'amount', 'createdAt', 'shoeSize', 'localCurrency', 'average_price_per_day',
            'days_since_release', 'release_date', 'rolling_average_price', 'color', 'model', 'transaction_date',
            'price_5_days_ago', 'price_15_days_ago', 'price_30_days_ago', 'price_5_days_future',
            'price_15_days_future', 'price_30_days_future']

        try:
            file_name = "shoe_data/shoe_transactions/{}/{}.csv".format(self.brand, shoe)
            shoe_transaction_df = pd.read_csv(file_name)
            #if data already has features
            if len(list(shoe_transaction_df.columns)) == len(expected_columns):
                pass
            else:
                print("Creating columns for: {}".format(file_name))
                self._get_average_price_per_day(shoe_transaction_df)
                self._add_rolling_mean(shoe_transaction_df)
                self._get_past_and_future_prices(shoe_transaction_df)
                shoe_transaction_df.to_csv(file_name, index=False)
        except:
            pass

    def add_additional_features(self):
        shoe_info_file_name = "shoe_data/{}_shoe_info.csv".format(self.brand.replace("-", "_"))
        shoe_info_df = pd.read_csv(shoe_info_file_name)
        
        if self.brand != "other-sneakers":
            self._format_model_col(shoe_info_df)
            self._format_name_col(shoe_info_df)
        self._add_color_info(shoe_info_df)
        self._add_date_info(shoe_info_df)
        self._add_blank_columns_to_info_df(shoe_info_df)

        shoe_names = self._get_shoe_name_list()
        self._add_shoe_info_features(shoe_info_df, shoe_names)
        shoe_info_df.to_csv(shoe_info_file_name, index=False)
        print("CSV file {} created".format(shoe_info_file_name))

        pool = Pool(processes=30)
        pool.map(self._add_additional_features_transaction_data, shoe_names)
        pool.close()
        pool.join()

        for shoe in shoe_names:
            self._drop_util_columns_from_df(shoe)

    def concatenate_transactions_data(self):
        path = "shoe_data/shoe_transactions/{}/".format(self.brand)
        all_filenames = [i for i in os.listdir(path)]

        for i, f in enumerate(all_filenames):
            try:
                if i == 0:
                    combined_csv = pd.read_csv(path + f)
                else:
                    combined_csv = pd.concat([combined_csv, pd.read_csv(path + f)], sort=True)
            except:
                continue
        output_filename = "shoe_data/{}_transactions.csv".format(self.brand.replace("-", "_"))
        combined_csv.to_csv(output_filename, index=False)
        print("CSV file {} created".format(output_filename))

if __name__ == '__main__':
    #adidas, nike, retro-jordans, other-sneakers
    extractor = StockXFeatureExtractor("adidas")

    #print("Adding additional features to the data:")
    #extractor.add_additional_features()

    #print("Creating a consolidated transactions file:")
    #extractor.concatenate_transactions_data()
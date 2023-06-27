import os
import json
import pandas as pd


class JsonProcessor:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.clm = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
                    'Transaction_amount': []}

    def process_jsons(self):
        states = os.listdir(self.folder_path)

        for state in states:
            state_path = os.path.join(self.folder_path, state)

            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)

                for quarter in os.listdir(year_path):
                    if quarter.endswith(".json"):
                        file_path = os.path.join(year_path, quarter)

                        with open(file_path) as json_file:
                            json_data = json.load(json_file)

                            for item in json_data['data']['transactionData']:
                                name = item['name']
                                count = item['paymentInstruments'][0]['count']
                                amount = item['paymentInstruments'][0]['amount']

                                self.clm['Transaction_type'].append(name)
                                self.clm['Transaction_count'].append(count)
                                self.clm['Transaction_amount'].append(amount)
                                self.clm['State'].append(state)
                                self.clm['Year'].append(year)
                                self.clm['Quarter'].append(int(quarter.strip('.json')))

        df = pd.DataFrame(self.clm)
        df["State"] = df["State"].astype("category")
        df["Year"] = df["Year"].astype("category")
        df["Transaction_type"] = df["Transaction_type"].astype("category")

        return df

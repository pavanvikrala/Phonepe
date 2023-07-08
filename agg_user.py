import os
import json
import pandas as pd


class JsonExtract:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.clm = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Count': [], 'Percentage': []}

    def process_jsons(self):
        # loop through the state folder
        states = os.listdir(self.folder_path)
        for state in states:
            state_path = os.path.join(self.folder_path, state)
            # Loop through the year folders
            for year in os.listdir(state_path):
                year_path = os.path.join(state_path, year)
                # Loop through quarter folders
                for quarter in os.listdir(year_path):
                    if quarter.endswith(".json"):
                        file_path = os.path.join(year_path, quarter)
                        with open(file_path) as json_file:
                            json_data = json.load(json_file)
                            if json_data['data']['usersByDevice'] is not None:

                                for item in json_data['data']['usersByDevice']:
                                    brand = item['brand']
                                    count = item['count']
                                    percentage = item['percentage']
                                    self.clm['Brand'].append(brand)
                                    self.clm['Count'].append(count)
                                    self.clm['Percentage'].append(percentage)
                                    self.clm['State'].append(state)
                                    self.clm['Year'].append(year)
                                    self.clm['Quarter'].append(int(quarter.strip('.json')))
                            else:
                                self.clm['Brand'].append(None)
                                self.clm['Count'].append(None)
                                self.clm['Percentage'].append(None)
                                self.clm['State'].append(state)
                                self.clm['Year'].append(year)
                                self.clm['Quarter'].append(int(quarter.strip('.json')))

        df = pd.DataFrame(self.clm)
        df['Count'].fillna(0, inplace=True)
        df.dropna(subset=["Brand"], inplace=True)
        df.dropna(subset=["Percentage"], inplace=True)
        df["State"] = df["State"].astype("category")
        df["Year"] = df["Year"].astype("category")
        df["Count"] = df["Count"].astype("float")
        df["Brand"] = df["Brand"].astype("category")
        df["Percentage"] = (df["Percentage"]*100).map('{:.2f}'.format).astype(float).astype(int)
        return df

import os
import json
import pandas as pd

class JsonData:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'RegisteredUsers': []}

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

                            for item in json_data['data']['districts']:
                                name = item['name']
                                users = item['registeredUsers']

                                self.clm['District'].append(name)
                                self.clm['RegisteredUsers'].append(users)
                                self.clm['State'].append(state)
                                self.clm['Year'].append(year)
                                self.clm['Quarter'].append(int(quarter.strip('.json')))

        df = pd.DataFrame(self.clm)
        df["State"] = df["State"].astype("category")
        df["Year"] = df["Year"].astype("category")
        df["District"] = df["District"].astype("category")
        return df
        

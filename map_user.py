import os
import json
import pandas as pd


class JsonSearch:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'AppOpens': [], 'Registered_Users': []}

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
                            my_dict = json_data['data']['hoverData']

                            for item in my_dict:
                                name = item
                                app_opens = my_dict[item]['appOpens']
                                reg_users = my_dict[item]['registeredUsers']

                                self.clm['District'].append(name)
                                self.clm['AppOpens'].append(app_opens)
                                self.clm['Registered_Users'].append(reg_users)
                                self.clm['State'].append(state)
                                self.clm['Year'].append(year)
                                self.clm['Quarter'].append(int(quarter.strip('.json')))
        df = pd.DataFrame(self.clm)
        df["State"] = df["State"].astype("category")
        df["Year"] = df["Year"].astype("category")
        df["District"] = df["District"].astype("category")
        return df

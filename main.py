from agg_trans import JsonProcessor
from agg_user import JsonExtract
from map_trans import JsonExplore
from map_user import JsonSearch
from top_trans_dist import JsonFind
from top_trans_pins import JsonDig
from top_user_dist import JsonData
from top_user_pins import JsonFetch
# import streamlit as st
import mysql.connector

# img_url = 'https://res.cloudinary.com/df7cbq3qu/image/upload/v1687743044/6iyA2zVz5PyyMjK5SIxdUhrb7oh9cYVXJ93q6DZkmx07Er1o90PXYeo6mzL4VC2Gj9s_sb5s84.png'
# page_config = {"page_title": 'Phonepe pulse', "page_icon": img_url, "layout": "wide"}
# st.set_page_config(**page_config)
# st.title("Welcome to the Phonepe pulse data visualization")


agg_trans_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/aggregated/transaction/country/india/state"
agg_trans = JsonProcessor(agg_trans_path)
agg_trans_df = agg_trans.process_jsons()

agg_user_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/aggregated/user/country/india/state"
agg_user = JsonExtract(agg_user_path)
agg_user_df = agg_user.process_jsons()


map_trans_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/map/transaction/hover/country/india/state"
map_trans = JsonExplore(map_trans_path)
map_trans_df = map_trans.process_jsons()

map_user_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/map/user/hover/country/india/state"
map_user = JsonSearch(map_user_path)
map_user_df = map_user.process_jsons()

top_trans_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/top/transaction/country/india/state"
top_trans_dist = JsonFind(top_trans_path)
top_trans_df = top_trans_dist.process_jsons()

top_trans_pinpath = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/top/transaction/country/india/state"
top_pins = JsonDig(top_trans_pinpath)
top_pins_df = top_pins.process_jsons()

top_user_path = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/top/user/country/india/state"
top_user_dist = JsonData(top_user_path)
user_dist_df = top_user_dist.process_jsons()

top_user_pinpath = "C:/Users/pavan/Documents/Phonepe_repository/pulse/data/top/user/country/india/state"
top_user_pins = JsonFetch(top_user_pinpath)
user_pin_df = top_user_pins.process_jsons()


connection = mysql.connector.connect(

                        host="localhost",
                        user="root",
                        password="Balaji123",
                        database='phonepe'
                    )

cursor = connection.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS agg_trans (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Transaction_type VARCHAR(100),
                        Transaction_count INT,
                        Transaction_amount FLOAT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS agg_user (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Brand VARCHAR(50),
                        Count INT,
                        Percentage INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS map_trans (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        Transaction_amount FLOAT,
                        Transaction_count INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS map_user (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        AppOpens INT,
                        Registered_users INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS top_trans_dist (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        Amount FLOAT,
                        Count INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS top_trans_pincodes (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Pincode VARCHAR(20),
                        Amount FLOAT,
                        Count INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_dist (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        Registered_users INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS top_user_pincodes (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Pincode VARCHAR(20),
                        Registered_users INT )
              ''')

# Inserting data into agg_trans table
agg_trans_query = ''' INSERT INTO agg_trans (
                            State,
                            Year,
                            Quarter,
                            Transaction_type,
                            Transaction_count,
                            Transaction_amount )
                       VALUES (%s, %s, %s, %s, %s, %s) '''

agg_trans_data = agg_trans_df[['State', 'Year', 'Quarter', 'Transaction_type',
                               'Transaction_count', 'Transaction_amount']].values.tolist()
cursor.executemany(agg_trans_query, agg_trans_data)

# Inserting data into agg_user table
agg_user_query = '''INSERT INTO agg_user (
                            State,
                            Year,
                            Quarter,
                            Brand,
                            Count,
                            Percentage )
                        VALUES (%s, %s, %s, %s, %s, %s) '''
agg_user_data = agg_user_df[['State', 'Year', 'Quarter', 'Brand', 'Count', 'Percentage']].values.tolist()
cursor.executemany(agg_user_query, agg_user_data)

# Inserting data into map_trans table
map_trans_query = '''INSERT INTO map_trans (
                            State,
                            Year,
                            Quarter,
                            District,
                            Transaction_amount,
                            Transaction_count )
                      VALUES (%s, %s, %s, %s, %s, %s)'''
map_trans_data = map_trans_df[['State', 'Year', 'Quarter', 'District',
                               'Transaction_amount', 'Transaction_count']].values.tolist()
cursor.executemany(map_trans_query, map_trans_data)

map_user_query = '''INSERT INTO map_user (
                            State,
                            Year,
                            Quarter,
                            District,
                            AppOpens,
                            Registered_Users )
                      VALUES (%s, %s, %s, %s, %s, %s)'''
map_user_data = map_user_df[['State', 'Year', 'Quarter', 'District', 'AppOpens', 'Registered_Users']].values.tolist()
cursor.executemany(map_user_query, map_user_data)


top_trans_dist_query = '''INSERT INTO top_trans_dist (
                            State,
                            Year,
                            Quarter,
                            District,
                            Amount,
                            Count )
                        VALUES (%s, %s, %s, %s, %s, %s)'''
top_trans_dist_data = top_trans_df[['State', 'Year', 'Quarter', 'District', 'Amount', 'Count']].values.tolist()
cursor.executemany(top_trans_dist_query, top_trans_dist_data)

top_trans_pins_query = '''INSERT INTO top_trans_pincodes (
                            State,
                            Year,
                            Quarter,
                            Pincode,
                            Amount,
                            Count )
                        VALUES (%s, %s, %s, %s, %s, %s)'''
top_trans_pins_data = top_pins_df[['State', 'Year', 'Quarter', 'Pincode', 'Amount', 'Count']].values.tolist()
cursor.executemany(top_trans_pins_query, top_trans_pins_data)

top_user_dist_query = '''INSERT INTO top_user_dist (
                            State,
                            Year,
                            Quarter,
                            District,
                            Registered_users )
                        VALUES (%s, %s, %s, %s, %s)'''
top_user_dist_data = user_dist_df[['State', 'Year', 'Quarter', 'District', 'RegisteredUsers']].values.tolist()
cursor.executemany(top_user_dist_query, top_user_dist_data)

top_user_pins_query = '''INSERT INTO top_user_pincodes (
                            State,
                            Year,
                            Quarter,
                            Pincode,
                            Registered_users )
                        VALUES (%s, %s, %s, %s, %s)'''
top_user_pins_data = user_pin_df[['State', 'Year', 'Quarter', 'Pincode', 'RegisteredUsers']].values.tolist()
cursor.executemany(top_user_pins_query, top_user_pins_data)

connection.commit()
cursor.close()
connection.close()

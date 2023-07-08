import pandas as pd
from agg_trans import JsonProcessor
from agg_user import JsonExtract
from map_trans import JsonExplore
from map_user import JsonSearch
from top_trans_dist import JsonFind
from top_trans_pins import JsonDig
from top_user_dist import JsonData
from top_user_pins import JsonFetch
import mysql.connector
import streamlit as st
import requests
import json
import plotly.express as px


img_url = 'https://res.cloudinary.com/df7cbq3qu/image/upload/v1687743044/6iyA2zVz5PyyMjK5SIxdUhrb7oh9cYVXJ93q6DZkmx07Er1o90PXYeo6mzL4VC2Gj9s_sb5s84.png'
page_config = {"page_title": 'Phonepe pulse', "page_icon": img_url, "layout": "wide"}
st.set_page_config(**page_config)
st.title("Welcome to the Phonepe pulse data visualization")


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
                        password="****",
                        database='phonepe'
                    )

cursor = connection.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS agg_trans (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Transaction_type VARCHAR(100),
                        Transaction_count FLOAT,
                        Transaction_amount FLOAT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS agg_user (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        Brand VARCHAR(50),
                        Count FLOAT,
                        Percentage INT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS map_trans (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        Transaction_amount FLOAT,
                        Transaction_count FLOAT )
              ''')

cursor.execute('''CREATE TABLE IF NOT EXISTS map_user (
                        State VARCHAR(50),
                        Year VARCHAR(4),
                        Quarter INT,
                        District VARCHAR(50),
                        AppOpens FLOAT,
                        Registered_users FLOAT )
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

url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
def state_df():
    """This function returns a list of states in alphabetical order"""
    response = requests.get(url)
    agg_trans_data = json.loads(response.content)
    states = [i['properties']['ST_NM'] for i in agg_trans_data['features']]
    states.sort()
    states_df = pd.DataFrame({'State': states})
    return states_df
states = state_df()

options = st.radio("Select your option 👇",
                   options=('Transaction data at country level', 'Total Transaction data at State level',
                            'Top 10 States/Districts/Pincodes with most transactions'), horizontal= True)
if options == 'Transaction data at country level':
    tab1, tab2 = st.tabs(['Transaction', 'User'])

    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            trans_year = st.selectbox('Select Year⤵️', ('2018', '2019', '2020', '2021', '2022'), key='trans_year')
        with col2:
            trans_quarter = st.selectbox('Select quarter🔽', ('1', '2', '3', '4'), key='trans_quarter')
        with col3:
            trans_type = st.selectbox("Select Payment Category⬇️", ('','Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'), key='trans_type')

        if trans_type:
            cursor.execute(f"SELECT State, SUM(Transaction_count) AS All_Transactions, "
                           f"SUM(Transaction_amount) AS Total_Transactions"
                           f" FROM agg_trans "
                           f"WHERE Year = '{trans_year}' AND Quarter = '{trans_quarter}' AND Transaction_type = '{trans_type}' "
                           f"GROUP BY State ORDER BY All_Transactions;")
            agg_trans_result = cursor.fetchall()
            agg_trans_result_df = pd.DataFrame(agg_trans_result,
                                               columns=['State', 'All Transactions', 'Total Transaction Amount'])

            # Create a choropleth map figure
            colorscale = ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#800000", "#008000",
                          "#000080", "#FFA500", "#FF4500", "#FF8C00", "#FFD700", "#ADFF2F", "#7FFF00",
                          "#32CD32", "#00FF7F", "#00FA9A", "#40E0D0", "#00CED1", "#0000CD", "#191970",
                          "#8A2BE2", "#4B0082", "#9932CC", "#FF1493", "#FF69B4", "#FFC0CB", "#FFE4E1",
                          "#FFFFF0", "#F5DEB3",'#FF0000', '#00FF00', '#0000FF', '#FFF800', '#FF00FF',
                          '#00FFFF', '#FFA500', '#800080', '#008000', '#000080']
            st.info("Now we create a choropleth map figure using Plotly Express. We can hover on to each state and get "
                    "required information like 'name of the state', 'Total transaction amount' and 'Total number of "
                    "transactions done'. ")
            agg_trans_fig = px.choropleth(
                data_frame=agg_trans_result_df,
                geojson=url,
                featureidkey='properties.ST_NM', locations=states['State'],
                color='Total Transaction Amount',
                color_continuous_scale=colorscale,
                hover_data=['All Transactions'],
                title='Choropleth Map🌏')

            agg_trans_fig.update_geos(fitbounds='locations', visible=False)
            agg_trans_fig.update_layout(title_font=dict(size=40), title_font_color='green', height=600)
            st.plotly_chart(agg_trans_fig, use_container_width=True)

            # agg_trans Bar chart
            if st.button("Click me to view horizontal bar chart 📊"):
                st.success("We create a horizontal bar graph with 'All the Transactions done' on X-Axis and "
                           "'Names of the States' on Y axis")
                agg_trans_bar_fig = px.bar(agg_trans_result_df, orientation='h',
                                           x='All Transactions', y='State',
                                           color='Total Transaction Amount', text='Total Transaction Amount',
                                           color_continuous_scale=px.colors.diverging.Portland_r,
                                           hover_data=['All Transactions', 'Total Transaction Amount'],
                                           title='Horizontal Bar Chart Analysis', height=800)
                agg_trans_bar_fig.update_layout(xaxis=dict(title_font=dict(size=18, color="blue"),
                                                           title_standoff=120))
                agg_trans_bar_fig.update_layout(yaxis=dict(title_font=dict(size=18, color="blue"),
                                                           title_standoff=50))
                agg_trans_bar_fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                agg_trans_bar_fig.update_layout(uniformtext_minsize=5, uniformtext_mode='hide')
                st.plotly_chart(agg_trans_bar_fig, use_container_width=True)

    with tab2:
        col1, col2, col3 = st.columns(3)
        with col1:
            user_year = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='user_year')
        with col2:
            user_quarter = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='user_quarter')
        with col3:
            brand = st.selectbox('Select brand', ('', 'Xiaomi', 'Samsung', 'Vivo', 'Oppo', 'OnePlus', 'Realme', 'Apple', 'Motorola',
                                                  'Lenovo', 'Huawei', 'Others', 'Techno', 'Gionee', 'Infinix', 'Asus', 'Micromax',
                                                  'HMD Global', 'Lava', 'COOLPAD', 'Lyf'), key='brand')
        if brand:
            st.info("Below we're viewing the Users data done by different phone brands at country level on a scatter plot. "
                    "'Total number of registered users' on X-axis and "
                    "'average percentage of market share' compared to other devices on Y-axis")
            cursor.execute(f"SELECT State, SUM(Count) AS Total_Users, round(AVG(Percentage)) AS Avg_marketshare_Percentage "
                           f"FROM agg_user "
                           f"WHERE Year = '{user_year}' AND Quarter = '{user_quarter}' AND Brand = '{brand}'"
                           f"GROUP BY State;")
            user_trans_result = cursor.fetchall()
            user_trans_result_df = pd.DataFrame(user_trans_result,
                                                columns=['State', 'Total Registered Users', 'Avg Market Share(%)'])
            if len(user_trans_result_df) == 0:
                st.error(f" {brand} brand has no data for the year {user_year} and Quarter {user_quarter} 😢")
            else :
                scatter_fig = px.scatter(data_frame=user_trans_result_df,
                                         x='Total Registered Users', y='Avg Market Share(%)',
                                         color='State',
                                         hover_data=['State', 'Total Registered Users', 'Avg Market Share(%)'],
                                         title='User Scatter Plot Analysis')

                scatter_fig.update_layout(title_font=dict(size=40),
                                          title_font_color='brown', height=600)

                scatter_fig.update_traces(marker=dict(size=10,
                                                      line=dict(width=1,
                                                                color='DarkSlateGrey'), opacity=0.7),
                                          selector=dict(mode='markers'))

                st.plotly_chart(scatter_fig, use_container_width=True)

elif options == 'Total Transaction data at State level':
    tab1, tab2 = st.tabs(['Transaction', 'User'])
    with tab1:
        if st.checkbox("Click me for info"):
            st.info("Here we deal with 'total number of transactions' and 'total value of all transactions' at the state level.")
        col1, col2, col3 = st.columns(3)
        with col1:
            map_year = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='map_year')
        with col2:
            map_quarter = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='map_quarter')
        with col3:
            map_state = st.selectbox('Select States', ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                                         'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                                         'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                                         'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                                         'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                                         'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                                         'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                                         'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                                         'uttarakhand', 'west-bengal'), key='map_states')
        if map_state:
            cursor.execute(f"SELECT District, SUM(Transaction_amount) AS Total_Transaction, "
                           f"SUM(Transaction_count) AS Total_Transaction_Count "
                           f"FROM map_trans "
                           f"WHERE Year = '{map_year}' AND Quarter = '{map_quarter}' AND State = '{map_state}' "
                           f"GROUP BY District;")

            map_trans = cursor.fetchall()
            map_trans_df = pd.DataFrame(map_trans, columns=['Location', 'Total Transaction Amount', 'Total Transaction Count'])

            map_trans_pie = px.pie(data_frame=map_trans_df,
                                   names='Location',
                                   values='Total Transaction Count',
                                   width=800, height=800
                                   )

            map_trans_pie.update_traces(hoverinfo=['label'])
            st.info(f"Here comes the PIE CHART which shows 'Total Transaction Count' in all the districts in {map_state}")
            st.plotly_chart(map_trans_pie, use_container_width=True)

    with tab2:
        if st.checkbox("Check me for info"):
            st.info(
                "Here we deal with 'Total number of registered users' and 'number of app opens by these registered users' "
                "at the state level.")
        col1, col2, col3 = st.columns(3)
        with col1:
            map_user_year = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='map_user_year')
        with col2:
            map_user_quarter = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='map_user_quarter')
        with col3:
            map_user_states = st.selectbox('Select States', ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                                         'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                                         'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                                         'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                                         'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                                         'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                                         'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                                         'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                                         'uttarakhand', 'west-bengal'), key='map_user_states')


        cursor.execute(f"SELECT REPLACE(District, ' district', '') AS Location, SUM(AppOpens) AS AppOpens, SUM(Registered_users) AS Registered_Users "
                       f"FROM map_user WHERE State = '{map_user_states}' AND year = '{map_user_year}' AND Quarter = '{map_user_quarter}'"
                       f"GROUP BY District ORDER BY Registered_Users;")
        map_user_result = cursor.fetchall()
        map_user_df = pd.DataFrame(map_user_result, columns=['Location', 'Total AppOpens by Users', 'Registered Users'])


        map_user_fig = px.bar(data_frame=map_user_df,
                              x='Total Number of Registered Users', y='Location', orientation='h',
                              height=600, color='Total AppOpens by Users', text='Registered Users',
                              color_continuous_scale=px.colors.qualitative.Pastel,
                              hover_data=['Total AppOpens by Users', 'Registered Users'],
                              title='Horizontal Bar Chart showing Total registered Users vs Total AppOpens')
        map_user_fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        map_user_fig.update_layout(uniformtext_minsize=5, uniformtext_mode='hide')
        st.plotly_chart(map_user_fig, use_container_width=True)

elif options == 'Top 10 States/Districts/Pincodes with most transactions':
    tab1, tab2, tab3, tab4 = st.tabs(['Top Transaction Districts', 'Top Transaction Pincodes', 'Top User Districts',
                                      'Top User Pincodes'])
    with tab1:
        if st.checkbox("Click me for info"):
            st.info(f"The Top 10 districts with the 'Total Transaction Count' & 'Total Transaction Amount'")
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='year')
        with col2:
            quarter = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='quarter')
        with col3:
            state = st.selectbox('Select States',
                                           ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                            'uttarakhand', 'west-bengal'), key='state')
        if state:
            cursor.execute(f"SELECT district, SUM(Count), SUM(Amount) "
                           f"FROM top_trans_dist "
                           f"WHERE State = '{state}' AND Year = '{year}' AND Quarter = {quarter} "
                           f"GROUP BY district;")
            top_trans_result = cursor.fetchall()
            top_trans_result_df = pd.DataFrame(top_trans_result,
                                               columns=['District', 'Total Transactions Count', 'Total Transaction Amount'])

            top_trans_pie = px.pie(data_frame=top_trans_result_df,
                                   names='District',
                                   values='Total Transactions Count',
                                   hover_data='Total Transaction Amount',
                                   title='Top Transaction Pie Chart',
                                   width=800, height=800, hole=.2
                                   )
            top_trans_pie.update_traces(hoverinfo=['label'])
            st.plotly_chart(top_trans_pie, use_container_width=True)

    with tab2:
        if st.checkbox("Check me for info"):
            st.info(f"The Top 10 Pincodes with the 'Total Transaction Count' & 'Total Transaction Amount'")
        col1, col2, col3 = st.columns(3)
        with col1:
            year2 = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='year2')
        with col2:
            quarter2 = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='quarter2')
        with col3:
            state2 = st.selectbox('Select States',
                                           ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                            'uttarakhand', 'west-bengal'), key='state2')

        if state2:
            cursor.execute(f"SELECT Pincode, SUM(Count), SUM(Amount) "
                           f"FROM top_trans_pincodes "
                           f"WHERE State = '{state2}' AND Year = '{year2}' AND Quarter = {quarter2} "
                           f"GROUP BY Pincode;")
            top_pincodes = cursor.fetchall()
            top_pincodes_df = pd.DataFrame(top_pincodes,
                                           columns=['Pincodes', 'Total Transactions Count', 'Total Transaction Amount'])

            top_pins_scatter = px.scatter(data_frame=top_pincodes_df,
                                          x='Total Transactions Count',
                                          y='Total Transaction Amount',
                                          color='Pincodes',
                                          size='Total Transaction Amount',
                                          log_x=True, size_max=60)

            st.plotly_chart(top_pins_scatter, use_container_width=True)

    with tab3:
        if st.checkbox("More info"):
            st.info(f"We can view the top 10 Districts with the most Registered Users")
        col1, col2, col3 = st.columns(3)
        with col1:
            year3 = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='year3')
        with col2:
            quarter3 = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='quarter3')
        with col3:
            state3 = st.selectbox('Select States',
                                           ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                            'uttarakhand', 'west-bengal'), key='state3')
        if state3:
            cursor.execute(f"SELECT District, SUM(Registered_users) "
                           f"FROM top_user_dist "
                           f"WHERE State = '{state3}' AND Year = '{year3}' AND Quarter = {quarter3} "
                           f"GROUP BY District;")
            top_user_result = cursor.fetchall()
            top_user_result_df = pd.DataFrame(top_user_result,
                                              columns=['District', 'Total Registered Users'])

            top_user_bar = px.bar(data_frame=top_user_result_df,
                                  x='District', y='Total Registered Users',
                                  color='District', height=600)
            st.plotly_chart(top_user_bar, use_container_width=True)

    with tab4:
        if st.checkbox("Get info"):
            st.info(f"The Top 10 Pincodes with the most registered users")
        col1, col2, col3 = st.columns(3)
        with col1:
            year4 = st.selectbox('Select Year', ('2018', '2019', '2020', '2021', '2022'), key='year4')
        with col2:
            quarter4 = st.selectbox('Select quarter', ('1', '2', '3', '4'), key='quarter4')
        with col3:
            state4 = st.selectbox('Select States',
                                           ('', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                            'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                            'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                            'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                                            'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
                                            'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
                                            'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                                            'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                            'uttarakhand', 'west-bengal'), key='state4')
        if state4:
            cursor.execute(f"SELECT Pincode, SUM(Registered_users) "
                           f"FROM top_user_pincodes "
                           f"WHERE State = '{state4}' AND Year = '{year4}' AND Quarter = {quarter4} "
                           f"GROUP BY Pincode;")
            top_user_pincodes = cursor.fetchall()
            top_user_pincodes_df = pd.DataFrame(top_user_pincodes,
                                                columns=['Top User Pincode', 'Total Registered Users'])
            unique_symbols = ['circle', 'square', 'diamond', 'cross', 'x', 'triangle-up', 'triangle-down', 'star',
                              'hexagon', 'pentagon']

            top_user_dot = px.scatter(data_frame=top_user_pincodes_df,
                                      x='Top User Pincode',
                                      y='Total Registered Users',
                                      symbol='Top User Pincode',
                                      symbol_sequence=unique_symbols,
                                      color='Top User Pincode')

            top_user_dot.update_layout(xaxis={'type': 'category'})
            top_user_dot.update_traces(marker_size=20)
            st.plotly_chart(top_user_dot, use_container_width=True)



connection.commit()
cursor.close()
connection.close()

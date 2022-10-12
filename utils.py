import constants
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime

def get_covid_links(link,last_file_date=None):

    html = requests.get(link)

    links = []

    for link in BeautifulSoup(html.text, parse_only=SoupStrainer("a"), features="html.parser"):
        if hasattr(link, "href") and link["href"].endswith(".csv"):
            url = f"{constants.GITHUB_LINK}{link['href']}".replace('/blob/', '/raw/')
            links.append(url)
    
    if last_file_date:
        links = [i for i in links if datetime.strptime(i.split("/")[-1].split(".")[0],"%m-%d-%Y") > datetime.strptime(last_file_date,"%m-%d-%Y")]

    return links

def get_covid_data(path):
    
    df = [pd.read_csv(i) for i in path]

    return pd.concat(df,axis=0)

links = get_covid_links(constants.COVID_MASTER_DATA_LINK,'10-01-2022')

df = get_covid_data(links)
df.columns = constants.COVID_COLUMNS
df = df.drop(columns=['Combined_Key'])

location_df = df[["FIPS","County","Province","Country","Latitude","Longitude"]]
location_df = location_df.apply(lambda col: col.str.lower() if col.dtype=="object" else col)
location_df = location_df.apply(lambda col: col.str.replace("[^a-zA-Z\s\(\)\-']","") if (col.dtype=="object" and col.str.isascii()==True) else col)
location_df = location_df.drop_duplicates()
location_df["Unique_Key"] = location_df.reset_index()
location_df.to_csv("./location_output.csv", index=False)
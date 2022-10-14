import constants
import datetime
import math
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from sqlalchemy import create_engine

class PfrData:
    def __init__(self,link,header):
        self.link = link
        self.header = header
        self.now = datetime.datetime.now()

    def raw_pfr_qb_data(self):
        data = requests.get(self.link).text

        soup = BeautifulSoup(data,"html.parser")
        table = soup.find("table", class_="sortable stats_table")

        headers = self.header
        rows = []

        for row in table.tbody.find_all('tr'):
            row_data = (row.find_all("th"))+(row.find_all("td"))
            stats = []

            for i in range(27):
                stats.append(row_data[i].text)

            rows.append(stats)

        df = pd.DataFrame(rows,columns=headers)

        return df

    def scrubbed_pfr_qb_data(self):
        df = self.raw_pfr_qb_data()
        df = df.replace(r'^\s*$', np.nan, regex=True)

        convert_dict = {
            "Games":int,
            "Games_Started":int,
            "Pass_Completions":float,
            "Pass_Attempts":float,
            "Pass_Yards":float,
            "Pass_Tds":float,
            "Pass_Ints":float,
            "Sacks":float,
            "Rush_Attempts":float,
            "Rush_Yards":float,
            "Rush_Tds":float,
            "Fantasy_Points":float,
            "DraftKings_Points":float,
            "FanDuel_Points":float,
            "Opponent_Rank":int,
            "Fantasy_Points_Allowed":float,
            "DraftKings_Points_Allowed":float,
            "FanDuel_Points_Allowed":float,
            "Fantasy_Points_Rank":int,
            "DraftKings_Points_Rank":int,
            "FanDuel_Points_Rank":int
        }

        df = df.astype(convert_dict)
        df["Player"] = df["Player"].str.replace(r"[\.']*","",regex=True)
        df["First_Name"] = df["Player"].str.extract(r"^(\w+)")
        df["Last_Name"] = df["Player"].str.extract(r"[\s](\w+)")
        df["Home_Away"] = df["Home_Away"].apply(lambda item: "away" if item=="@" else "home")
        df["Snaps_%"] = df["Snaps"].str.extract(r"([0-9]*\.[0-9]*)").astype("float64")
        df["Snaps"] = df["Snaps"].str.replace(r"(\([0-9]*.[0-9]*%\))","",regex=True).astype("int64")
        df["Position"] = "qb"
        df["Week_Number"] = self.calculate_nfl_week()
        df = df.apply(lambda col: col.str.lower() if col.dtype=="object" else col)

        return df
    
    def calculate_nfl_week(self):
        year = self.now.year
        schedule_start = datetime.datetime.strptime(constants.NFL_SCHEDULE[year]["start"],"%Y-%m-%d")
        date_diff = abs(self.now-schedule_start)
        days = date_diff.total_seconds()/86400

        return math.ceil(days/7)

    def pfr_data_to_mysql(self,user,password,host,port,database,table):
        engine = create_engine(f"mysql://{user}:{password}@{host}:{port}/{database}")

        df = self.scrubbed_pfr_qb_data()
        return df.to_sql(table,con=engine,if_exists='append',index=False,method="multi")

if __name__ == "__main__":

    qb_data = PfrData(constants.PFR_QB_MATCHUP_LINK,constants.PFR_QB_MATCHUP_HEADER)
    qb_data.pfr_data_to_mysql(user="",password="",host="",port="",database="",table="")
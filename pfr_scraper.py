import constants
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer

def get_pfr_qb_data(link):
    data = requests.get(link).text

    soup = BeautifulSoup(data,"html.parser")
    table = soup.find("table", class_="sortable stats_table")

    headers = constants.PFR_QB_MATCHUP_HEADER
    rows = []

    for row in table.tbody.find_all('tr'):
        row_data = (row.find_all("th"))+(row.find_all("td"))
        stats = []

        for i in range(27):
            stats.append(row_data[i].text)

        rows.append(stats)

    df = pd.DataFrame(rows,columns=headers)
    
    return df

if __name__ == "__main__":

    print(get_pfr_qb_data(constants.PFR_QB_MATCHUP_LINK))
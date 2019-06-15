import requests
from bs4 import BeautifulSoup 
import pandas as pd

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import date

import json
import time

def get_race_by_link(link, tries=0):
    res = requests.get(link)
    if res.status_code != 200:
        if res.status_code == 400 or tries > 5:
            print("res.status_code = " + str(res.status_code))
            print(link)
            print("GET Failed")
            return False
        else:
            return get_race_by_link(link, tries + 1)

    soup = BeautifulSoup(res.content, "lxml")
    result = []
    table = soup.find("table")
    for row in table.find_all("tr"):
        result.append([])
        for cell in row.find_all("td"):
            result[-1].append(cell.get_text())
    result = result[1:]
    return result

def get_race_dates():
    driver=webdriver.Firefox()
    driver.get("https://www.scmp.com/sport/racing/race-result/")
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "ui-datepicker-trigger")))
    dp = driver.find_element_by_class_name("ui-datepicker-trigger")
    dp.click()

    result = []
    prev_page_exists = True
    while prev_page_exists:
        soup = BeautifulSoup(driver.page_source, "lxml")
        for calendar in soup.find_all("table", class_="ui-datepicker-calendar"):
                for td in calendar.find_all("td", class_="ui-state-enabled"):
                    year= int(td['data-year'])
                    month=int(td['data-month'])+1
                    day= int(td.find('a').get_text())
                    print([year,month,day])
                    result.append(date(year, month,day))
        prev_btn=driver.find_element_by_class_name("ui-datepicker-prev")
        if "ui-state-disabled" in prev_btn.get_attribute("class"):
            prev_page_exists = False
        else:
            prev_btn.click()
    
    return result 

def save_race_dates_to_csv():
    dates = get_race_dates()
    df = pd.DataFrame({"dates": dates})
    df.to_csv("race_dates.csv", index=False)

def scrape_by_dates(dates):
    result = []
    for n_date, d in enumerate(dates):
        if not n_date % 10:
            time.sleep(4)
        # path_str = str(d.year) + str(d.month) + str(d.day)
        path_str=d
        res = requests.get("https://www.scmp.com/sport/racing/race-result/"+path_str+"/1")
        if res.status_code != 200:
            print(d)
            print(path_str)
            print(res.status_code)
            print("GET Failed")
            return False

        soup = BeautifulSoup(res.content, "lxml")
        num_races = len(soup.find("ul",class_="lists").find_all("li"))
        print(str(n_date)+"th date of chunk, " + str(num_races) + " races")

        for i in range(1, num_races+1):
            link = "https://www.scmp.com/sport/racing/race-result/"+path_str+"/"+str(i)
            standing = get_race_by_link(link)
            if not standing:
                f= open("missing.txt","w+")
                f.write(link + "\n")
                f.close()
                continue
            for row in standing:
                row.append(i)
                row.append(d)
                result.append(row)                                                  
    return result

def save_race_results_to_csv():
    df = pd.read_csv("race_dates.csv")
    dates = df['dates'].tolist()
    dates = [s.replace("-", "") for s in dates]
  
    CHUNK_SIZE = 100
    chunked_dates = [dates[i:i+CHUNK_SIZE] for i in range(0, len(dates), CHUNK_SIZE)]
    for n_chunk, chunk in enumerate(chunked_dates):
        if n_chunk > 9:
            results = scrape_by_dates(chunk)
            df = pd.DataFrame(results)
            df.to_csv("all_race_results_" + str(n_chunk) + ".csv")
            print(str(n_chunk) + " races scraped!")

def concat_results():
    dfs = [pd.read_csv("all_race_results_"+str(i)+".csv") for i in range(0, 14)]
    result = pd.concat(dfs)
    result.to_csv("all_race_results.csv")
    
if __name__ == "__main__":
    concat_results()    

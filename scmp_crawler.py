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

def scrape_horse_profile_links():
    def get_links(page_source):
        result = []
        soup = BeautifulSoup(page_source, "lxml")
        for div in soup.findAll("div", class_="result-rows"):
            result.append(div.find("a")['href'])
        return result
    url = "https://www.scmp.com/sport/racing/stats/horses"
    driver=webdriver.Firefox()
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "search-index")))
    aplha_selector = driver.find_element_by_class_name("search-index")
    links = []
    links.extend(get_links(driver.page_source))

    for i in range(2, 27):
        btn = aplha_selector.find_element_by_xpath(f"li[{i}]")
        btn.click()
        links.extend(get_links(driver.page_source))
        get_links(driver.page_source)
    driver.close()
    return links

def scrape_horse_profile(link):
    print(link)
    time.sleep(2)
    res = requests.get(link)
    if res.status_code != 200:
        print(f"{res.status_code}")
        raise requests.HTTPError
    soup = BeautifulSoup(res.content, "lxml")
    info = soup.find("div", class_ = "profile-panel").find("div", class_ = "wrapper")
    header = info.find("div", class_ = "header")
    name = header.h1.get_text().split("(")[0].strip()
    code = header.h1.get_text().split("(")[1].split(")")[0]
    trainer = header.h2.get_text().split("/")[0].strip()
    rating = header.h2.get_text().split(":")[1].strip()
    details = info.find("div", class_="details")
    p=details.get_text().split("\n")

    result = {}
    for line in p:
        print(line)
        if line.startswith("Import Type / Colour / Sex / Age / Country of Origin"):
            genes = line.split(":")[1]
            words = [s.strip() for s in genes.split("/")]
            result["import_type"] = words[0]
            result["color"] = words[1]
            result["sex"] = words[2]
            result["age"] = words[3]
            result["country_of_origin"] = words[4]
        elif line.startswith("Bloodline Relations:"):
            result["relations"] = line.split(":")[1]
        elif line.startswith("Owner:"):
            result["owner"] = line.split(":")[1]
        elif line.startswith("Sire:"):
            result["sire"] = line.split(":")[1]
        elif line.startswith("Dam:"):
            result["dam"] = line.split(":")[1]
        elif line.startswith("Health:"):
            result["health"] = line.split(":")[1]

    # print(f"{name}\n{code}\n{trainer}\n{rating}\n{p}")
    # print(f"Parsed\n{relations}\n{owner}\n{sire}\n{dam}\n{genes}\n{words}\n{import_type}\n{color}\n{sex}\n{age}\n{country_of_origin}")
    return result

def scrape_all_horse_profiles(start=0):
    try:    
        df = pd.DataFrame()
        links = scrape_horse_profile_links()
        for link in links[start:]:
            horse_profile = scrape_horse_profile("https://www.scmp.com" + link)
            df.append(horse_profile, ignore_index=True)
            start=start+1
        df.to_csv("data/horse_profiles.csv", index=False)
    except requests.HTTPError:
        time.sleep(8)
        df.to_csv(f"data/horse_profiles{start}.csv", index=False)
        scrape_all_horse_profiles(start=start)
if __name__ == "__main__":
    scrape_all_horse_profiles(start=278)
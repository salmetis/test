from bs4 import BeautifulSoup
import requests
import csv
import psycopg2
import pandas as pd
import pymongo
import json
import os




my_url = requests.get('https://www.reuters.com/theWire').text

soup = BeautifulSoup(my_url, 'lxml')

csv_file = open('scrape.csv', 'w')

csv_writer = csv.writer(csv_file)
csv_writer.writerow(['URL', 'Title', 'Short Description', 'Datetime'])

for article in soup.find_all('article'):
    title = article.h3.text.strip()  
    print(title)


    description = article.find('div', class_='story-content').p.text
    print(description)


    datetime = article.find('span',{'class':'timestamp'}).get_text().strip()
    print(datetime)       
 

    urlw = article.find('div', class_='story-content').a['href'] 
    url = 'https://www.reuters.com' + urlw
    print(url)

    print()

    csv_writer.writerow([url, title, description, datetime])

csv_file.close()


connection = psycopg2.connect(user = "postgres",
                                  password = "13371488",
                                  host = "localhost",
                                  port = "5432",
                                  database = "postgres")
cursor = connection.cursor()


create_table_query = '''CREATE TABLE scrappy13
(url  TEXT NOT NULL ,
title  TEXT NOT NULL ,
description  TEXT NOT NULL ,
datetime TEXT NOT NULL);'''

cursor.execute(create_table_query)
connection.commit()
f = open(r'scrape.csv', 'r')
cursor.copy_from(f, 'scrappy13', sep=',')
f.close()

def import_content(filepath):
    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['scrap'] 
    collection_name = 'scrappy' 
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.remove()
    db_cm.insert(data_json)

if __name__ == "__main__":
  filepath = 'scrape.csv'  
  import_content(filepath)

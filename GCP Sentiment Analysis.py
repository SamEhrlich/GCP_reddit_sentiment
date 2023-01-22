#!/usr/bin/env python
# coding: utf-8

# This project uses GCP's VM instance to scrape reddit's RSS feed into GCP's NLP. Then the data that is scraped along with the processed text is transferred into a PostgreSQL database held on GCP's platform. Then Python is used to connect to the database, query the database and visualize the data. 
# 
# Step 1 is to create a VM instance through the compute engine tab on the GCP site
# Step 2 is to allow the VM access to the NLP and begin creating a script within the VM using vim
--code to setup VM with python, pip and packages for scraping (This assumes a VM and a bucket is created under the same project)

sudo apt update
sudo apt install python3-venv python3-pip

pip3 install feedparser
pip3 install google-cloud
pip3 install google-cloud-vision
pip3 install --upgrade google-cloud-storage
pip3 install google-cloud-language

--allow google API to access VM

gcloud auth application-default login

--create script to scrape 

vim name_of_script.py

--functions inside vim

press "i" to insert code into vim and "esc" to exit insert mode
:w --writes 
:q --quits out of script and back to VM
:wq --writes and then quits
:!python3 name_of_script.py --runs script within vim OR outside of vim just type 'python3 name_of_script.py'
 --script used inside of VM to scrape reddit RSS feed into NLP

import feedparser #parses rss feeds including twitter/fb/reddit
from bs4 import BeautifulSoup #scrape data
from bs4.element import Comment 
from google.protobuf.json_format import MessageToDict #change json format from NLP into dictionary format
from google.cloud import language #NLP
import json
import pandas as pd
import os

--get only the visible text elements from the feed
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

--get the body from the html
def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)
    
--url of reddit (get 100 most recent posts)
a_reddit_rss_url = 'https://www.reddit.com/r/leagueoflegends/.rss?limit=100&after=t3_zms68r'
feed = feedparser.parse(a_reddit_rss_url)

--initialize an empty list to store data coming in
posts = []

if (feed['bozo'] == 1):
    print("Error Reading/Parsing Feed XML Data")
else:
    for item in feed[ "items" ]:
        date = item[ "date" ][0:10]
        title = item[ "title" ]
        summary_text = text_from_html(item[ "summary" ])
        link = item[ "link" ]
        lang = language.LanguageServiceClient()
        document = language.Document(content = summary_text, type_ = language.Document.Type.PLAIN_TEXT)
        response = lang.analyze_sentiment(document = document)
        result_dict = MessageToDict(response._pb)
        result_dict['documentSentiment'].setdefault('score',0) #if no score is listed set the default to 0
        result_dict['documentSentiment'].setdefault('magnitude',0) #if no magnitude is listed set the default to 0
        d = {'date' : date, 'title' : title, 'text' : summary_text, 'link' : link,
            'sent_score' : result_dict['documentSentiment']['score'],
            'magnitude' : result_dict['documentSentiment']['magnitude']}
        posts.append(d)
        
--initialize variables for connecting and loading data into database
project_id = 'project_id'
region = 'us_central1'
instance_name = 'instance_name'

INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}"

DB_USER = 'sam_ehrlich'
DB_PASS = 'postgres'
DB_NAME = 'name_of_db'

--connect and load data into database using sqlalchemy

from google.cloud.sql.connector import Connector
import sqlalchemy

--initialize connector
connector = Connector()

--function to return database connector object
def getconn():
    conn =  connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        )
    return conn

--create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

--connect and create table 
with pool.connect() as db_conn:
    db.conn.execute(
    "CREATE TABLE IF NOT EXISTS ratings "
    "(id SERIAL NOT NULL, date VARCHAR(12), title VARCHAR(256) NOT NULL, "
    "text TEXT, link VARCHAR(255), sent_score FLOAT, magnitude FLOAT "
    "PRIMARY KEY (id));"
    )
    
--insert data into our new table
    insert_statement = sqlalchemy.text(
    "INSERT INTO ratings (date, title, text, link, sent_score, magnitude) VALUES \
    (:date, :title, :text, :link, :sent_score, :magnitude)",
    )
    
    for each in posts:
        db.conn.execute(insert_statement, date = each['date'], title = each['title'], \
        text = each['text'], link = each['link'], sent_score = each['sent_score'], \
        magnitude = each['magnitude'])

--query database
    results = db.conn.execute("SELECT * FROM ratings LIMIT 5;").fetchall()

--print results of query
    for row in results:
        print(row)
# Once the data has been scraped and loaded from within the VM, anyone with a connection to the database can query the data. 

# In[ ]:


#connect to db through jupyter notebook
import getpass
mypasswd = getpass.getpass()


import psycopg2
connection = psycopg2.connect(database = 'db_name', 
                              user = 'user', 
                              host = '99.999.999.99', # Replace with SQL IP
                              password = 'password')
with connection, connection.cursor() as cursor:
    cursor.execute("SELECT * FROM ratings")
    results = cursor.fetchall()
    for row in results:
        print(row)


# In[ ]:


#query db using pandas
import pandas as pd

SQL = "SELECT * "
SQL += " FROM reddit_table "
SQL += " "

print(SQL)
reddit_df = pd.read_sql(SQL,connection)
reddit_df.head()


# In[ ]:


#convert date to datetime object
import datetime as dt

reddit_df['date'] = pd.to_datetime(reddit_df['date'], format ='%Y/%m/%d')


# In[ ]:


#assign a net positive/negative/neutral tag to each post
reddit_df['sentiment'] = 'NEU'
reddit_df.loc[reddit_df['sent_score'] >= 0.25, 'sentiment'] = 'POS'
reddit_df.loc[reddit_df['sent_score'] <= -0.25, 'sentiment'] = 'NEG'


# In[ ]:


#visualize distribution of post's sentiment
import seaborn as sns

sns.countplot(x=reddit_df["sentiment"])


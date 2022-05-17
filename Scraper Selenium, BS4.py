#Scraper Selenium, BS4

# In[1]:


#Directory, pandas, numpy, time, regex, directory
import os 
import pandas as pd
import numpy as np 
from tqdm import tqdm
import time
import re 

#Requests/BS4
import requests
from bs4 import BeautifulSoup

#Selenium + error exceptions  
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service

#Connection to ArangoDB 
import getpass
import configparser
from pyArango.connection import *
from IPython.display import clear_output


#ARANGODB CONNECTION

# In[2]:


#First open the Arango application on your desktop, otherwise a connection error will occur
username = input('Please enter your username: ')
password = getpass.getpass('Please enter your password: ')
connect = Connection(username=username, password=password)
clear_output(wait=True)
print('Connected to ArangoDB')
del password  


# In[3]:


#Create a new Database (only needs to be done once)
#db = connect.createDatabase(name="ThesisDSBA")
#Connect to the new created database
Database = connect["ThesisDSBA"]


# In[4]:


#Create Collection (only needs to be done once)
#CompanyText = Database.createCollection(name="CompanyText")
#Connect to that collection
CompanyText = Database["CompanyText"]
ECOOM = Database["ECOOM"]


#SCRAPING

# In[5]:


#Extract all urls that are not None 
Urls_all = []
for E in ECOOM.fetchAll():
    if E["Website"] is not None:
        Urls = Urls_all.append(E["Website"]) 
        
Firms_all = []    
for E in ECOOM.fetchAll():
    if E["Website"] is not None:
        Firms = Firms_all.append(E["Firm"])


# In[6]:


#Put URLs into the right format 
def convert_url(url):
    if('http' in url) & (url[-1:] == '/'):
        return url
    elif ('http' in url) & (url[-1:] != '/'):
        return url + '/'
    elif ('http' not in url) & (url[-1:] == '/'):
        return 'http://' + url
    else:
        return 'http://' + url + '/'


#URL VALIDATION CHECK

# In[7]:


#Check if URL is valid...

#Wrong URLs
Wrongurl = []
Wrong_firms = []

#URLs list
Righturl= []
Firms = []

myrequest = requests.Session()

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
          "Accept-Encoding": "gzip"}

for url in tqdm(Urls_all):
    try:
        fixed_url =  convert_url(url)
        response = myrequest.get(fixed_url, timeout = (5,10), headers=headers, verify=True, allow_redirects=True)
        
        if response.status_code == 200 or response.status_code == 304:
            print(fixed_url)
            print("URL is valid")
            Righturl.append(fixed_url)
            index = Urls_all.index(url)
            Firms.append(Firms_all[index]) 
        
        
        elif response.status_code==301 or response.status_code == 403 or response.status_code == 404 or response.status_code == 410:
            print(fixed_url)
            print("URL does not exist on Internet")
            index = Urls_all.index(url)
            Wrong_firms.append(Firms_all[index])
            Wrongurl.append(fixed_url)
        else: 
            index = Urls_all.index(url)
            Wrong_firms.append(Firms_all[index])
            Wrongurl.append(fixed_url)
            
#200: Succes status response  
#304: Not modified since last time accessed (happened sometimes if the website was visited before.) 
#301: Moved permanently
#403: Forbidden
#404: Not found
#410: Gone
#Possibility to add more errors

#In case an error happens, the loop will continue.  
    except (requests.exceptions.ConnectionError, requests.exceptions.InvalidURL, requests.exceptions.TooManyRedirects, requests.exceptions.Timeout,  requests.exceptions.ReadTimeout, requests.exceptions.ContentDecodingError):
        print(fixed_url)
        print("URL does not exist on Internet")
        index = Urls_all.index(url)
        Wrong_firms.append(Firms_all[index])
        Wrongurl.append(fixed_url)
        continue


#SCRAPE URL

# In[8]:

import time
wait=time.sleep(10)


# In[9]:


#Scrape the Actual URL
#Afterwards those websites are added into Arango. 

myrequest = requests.Session()

#Text
data = []  
#URL
urllist = []
#Firmname
Firmlisturl = []
#URL that could not be scraped
unlabeledurl = []

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36", "Accept-Encoding": "gzip"}

for url in tqdm(Righturl):
    try:
        driver = webdriver.Chrome('PATHNAME CHROMEDRIVER')
        driver.implicitly_wait(10)
        driver.get(url)
        wait
        source = driver.page_source
        soup = BeautifulSoup(source, 'html.parser')
        
        #removes all script and style tags
        for script in soup(["style", "script"]):
            script.decompose()
            
        #if no "body" tag in the html text, the firms will be classified as unlabeled
        if soup.find("body") == None: 
            unlabeledurl.append(url) 
                
        else: 
            html = soup.find("body").get_text()
            html = " ".join(html.split())
            #In few cases words were attached to each other, here these words were splitted
            html = re.sub(r"(?<![A-Z])(?<!^)([A-Z])",r" \1",html)
            data.append(html)
            print("scrape url " + url)
            urllist.append(url)
            index = Righturl.index(url)
            Firmlisturl.append(Firms[index])
                   
    except WebDriverException:
        print("WebDriverException")
        unlabeledurl.append(url)
        continue


#DISPLAY SCRAPED TEXT

# In[10]:


import time
for i in range(0, len(data)):
    print(i)
    print()
    print()
    print(Firmlisturl[i])
    print(urllist[i])
    print(data[i])
    print()
    print()
    print()


#ARANGODB STORAGE

# In[11]:


#Put Data into Arango
DataFrame_Companies = pd.DataFrame({"Firm": Firmlisturl , 'website' : urllist,
                                'txt' : data})


# In[12]:


#Connect to the right Database
db = connect["ThesisDSBA"]


# In[13]:


#Create a new Database Collection, This only needs to be done once! Can be neglected in the next run
#Companies_Flanders = db.createCollection(name="CompaniesFlanders")


# In[14]:


#Connect to the right Collection
CompaniesFlanders = db["CompanyText"]


# In[15]:


#Upload Data based on Key value pairs and give it another Name
for index, row in DataFrame_Companies.iterrows():
    doc = CompaniesFlanders.createDocument()
    if not (pd.isnull(row["Firm"])):    
        doc['Firm'] = row["Firm"] 
    if not (pd.isnull(row["website"])):    
        doc['website'] = row["website"] 
    if not (pd.isnull(row["txt"])):    
        doc['txt'] = row["txt"] 
    doc.save()








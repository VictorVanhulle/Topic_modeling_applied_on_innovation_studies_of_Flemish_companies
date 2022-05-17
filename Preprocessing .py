#Preprocessing

# In[1]:


#Connection to ArangoDB 
import getpass
import configparser
from pyArango.connection import *
from IPython.display import clear_output

#Language Detector
from langdetect import detect

#Preprocessing packages nltk
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.stem import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

#Other
import re
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


# In[2]:


#Preprocess lowercase, punctuation, stopwords, lemmatization, stemming
def preprocessing(text, stopwords = None, stemming = False, lemmatization = True, cookies = None, other = None):
    #cleaning
    text = re.sub(r'[^\w\s]', " ", str(text).lower().strip())
    text = re.sub(r'\d+', " ", str(text))

    
    #Tokenization of words
    list_companies_text = text.split()
    
    #Stopwords
    if stopwords is not None:
        list_companies_text = [word for word in list_companies_text if word not in stopwords]
        
    #Cookies
    if cookies is not None:
        list_companies_text = [word for word in list_companies_text if word not in cookies]
    
    #Other words
    if other is not None:
        list_companies_text = [word for word in list_companies_text if word not in other]
        
    #Stemming
    if stemming == True:
        stem_ = SnowballStemmer("dutch")
        list_companies_text = [stem_.stem(word) for word in list_companies_text]
        
    #Lemmatization
    if lemmatization == True:
        lemmatize = WordNetLemmatizer()
        list_companies_text = [lemmatize.lemmatize(word) for word in list_companies_text]
   
        
    text = " ".join(list_companies_text)
    return text


# In[3]:


#Text Average
def Average(lst):
    return sum(lst) / len(lst)


# In[4]:

#Cookie words
cookiess = ['accepteer', 'accepteren', 'accept', 'advertenties', 'advertisements', 'analyse', 'analyze', 'analytics',
            'analytische','analytisch','bepaalde', 'certain', 'bezoeker','visitor','belgian', 'browser','cookies','cookie','copyright',
            'choose','kies','kiezen', 'delen', 'share','derden','third', 'parties','party', 'disclaimer', 'functioneel','functional',
            'functionele','functioneren','function','gebruik', 'gebruiker','gebruikt', 'use', 'user','used','inhoud',
            'content','instellingen','settings', 'klikken','click','login','register','registreer','necessary','noodzakelijk', 
            'noodzakelijke','opgeslagen', 'opslaan', 'save','saved','pagina','page','policy','beleid','privacy', 'relevante',
            'relevant','social','sociaal','sociale', 'store','opslaan','toestemming','consent', 'voorkeuren','preference',
            'preferences','website', 'websites','algemene','voorwaarden', 'aanmelden','account','gegevens', 'www', 'com', 
            'contact', 'contacteer','websites','gebruiken','this', 'these', 'that', 'cookieverklaring', 'toggle', 'more', 'about', 'rights',
            'reserved', 'privacy', 'support','ondersteuning', 'copyright', 'cookiebeleid']


# In[5]:

#Languages + others
others = ['deutsch','duits','german','germany', 'dutch','nederlands','engels', 'english','francais','français','frans', 'french',
          'france','belgië','belgie','belgium','nederland','netherlands','duitsland','espanol','language', 'benelux',
          'languages','talen','taal','select','selecteer', 'selecteren', 'united','kingdom',
          'verenigd','koninkrijk', 'afspraak','maandag','dinsdag','woensdag','donderdag','vrijdag','zaterdag',
          'zondag','gesloten','openingsuren','instagram','youtube', 'linkedin', 'twitter','facebook','januari', 'februari', 'maart',
          'april','mei','juni','juli','augustus','september','oktober','november','december', 'afspraak','meeting','appointment',
          'monday','tuesday','wednesday','thursday','friday','saturday','sunday','openingsuren','opening','hours','gesloten', 
          'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'oktober', 'november', 'december', 'vlaanderen'] 


# In[6]:


#First open the Arango application on your desktop, otherwise a connection error will occur
username = input('Please enter your username: ')
password = getpass.getpass('Please enter your password: ')
connect = Connection(username=username, password=password)
clear_output(wait=True)
print('Connected to ArangoDB')
del password


# In[7]:


#Connect to the right Database
db = connect["ThesisDSBA"]


# In[8]:


#Connection to the right collection
Companytext_Businesses = db["Companytext_Businesses"]


# In[9]:


#Get Data out of ArangoDB 
Company = []
Text = []
for b in Companytext_Businesses.fetchAll():
    Company.append(b["Firm"])
    Text.append(b["txt"])


# In[10]:


#Make it a Dataframe
Firms = pd.DataFrame(list(zip(Company, Text)), columns =['Company', 'Text'])


# In[11]:


#The actual preprocessing of the data + printing 
Firms["preprocessed"] = Firms["Text"].apply(lambda x:
                                              preprocessing(x, stopwords = None, lemmatization = False, stemming = False, cookies=cookiess, other=others))


# In[12]:


#Stopwords removed from the text 
stopwords_English = stopwords.words("English")   
stopwords_Dutch = stopwords.words("Dutch")
stopwords_all = stopwords_Dutch + stopwords_English


# In[13]:


#Delete Firms with less than 20 words
for i in range(0,len(Firms.Company)):
    if len(Firms.Text[i].split()) <= 20:
        print(Firms.Company[i])
        print(len(Firms.Text[i].split()))


# In[14]:


#Delete domain names for sale
words = ["domain", "domein", "sale", "domeinnaam", "domainname", 'koop']
for i in range(0,len(Firms.Company)):
    if any(x in Firms.Text[i] for x in words):
        print()
        print(i)
        print(Firms.Company[i])
        print(Firms.Text[i])
        print()
        print()


# In[15]:


#Connect to the right Collection
DataFirms = db["DataFirms"]


# In[16]:


#Get Data out of ArangoDB
Firm = []
Inno = []
Text = []
zipActivPost19 = []
for b in DataFirms.fetchAll():
    Firm.append(b["Firm"])
    Inno.append(b["Inno"])
    Text.append(b["Text"])
    zipActivPost19.append(b["zipActivPost19"])


# In[17]:


#Language Detection
list_language = []

for i in range(0, len(Text)):
    try:
        language = detect(Text[i])
        list_language.append(language) 
    except:
        language = "error"
        list_language.append(language)
        continue 


# In[20]:

#Filter on English and Dutch Text
Company_list = {"Firm": Firm, "Inno": Inno, "Text": Text,  "zipActivPost19":zipActivPost19,  "Language": list_language}
Company_df = pd.DataFrame(Company_list)
Company_Dutch = Company_df[Company_df.Language == "nl"]
Company_English = Company_df[Company_df.Language == "en"]


# In[21]:


#Plot
Languages = pd.DataFrame({'language': list_language})

for lang in Languages:
    Languages[lang].value_counts().sort_values(ascending=False).plot(kind='bar', rot=0,  ylabel='count')
    plt.show()


# In[22]:


#Connect to the right Collection
CompanyDutch = db["CompanyDutch"]


# In[23]:


#Get Data out of ArangoDB
Firm = []
Inno = []
Text = []
zipActivPost19 = []
for b in CompanyDutch.fetchAll():
    Firm.append(b["Firm"])
    Inno.append(b["Inno"])
    Text.append(b["Text"])
    zipActivPost19.append(b["zipActivPost19"])
CompanyDutch = {"Firm": Firm, "Inno": Inno, "Text": Text, "zipActivPost19": zipActivPost19 }
CompanyDutch = pd.DataFrame(CompanyDutch)


# In[24]:


#The actual preprocessing of the data
CompanyDutch["preprocessed"] = CompanyDutch["Text"].apply(lambda x:
                                              preprocessing(x, stopwords = stopwords_all, lemmatization = False, stemming = False, cookies=cookiess, other=others))


# In[25]:


#Drop Duplicates
CompanyDutch = CompanyDutch.drop_duplicates()
CompanyDutch = CompanyDutch.dropna(subset=['Inno'])


# In[26]:


len(CompanyDutch)


# In[27]:


#More Preprocessing
noninnovative = CompanyDutch[CompanyDutch["Inno"] == 0.0]
innovative = CompanyDutch[CompanyDutch["Inno"] == 1.0]


# In[28]:


#Set indices again
indices = np.arange(0,len(innovative) , 1).tolist()
innovative["indices"] = indices
innovative = innovative.set_index('indices')
indices = np.arange(0,len(noninnovative) , 1).tolist()
noninnovative["indices"] = indices
noninnovative = noninnovative.set_index('indices')


# In[29]:


#Length innovative
length_innovative = []
for i in range(0, len(innovative)):
    length_innovative.append(len(innovative.preprocessed[i].split()))


# In[30]:


#Length non-innovative
length_noninnovative = []
for i in range(0, len(noninnovative)):
    length_noninnovative.append(len(noninnovative.preprocessed[i].split()))


# In[31]:


#Average
Average(length_noninnovative)


# In[32]:


#Average
Average(length_innovative)


# In[33]:


#Every time a preprocessing step was done, a new collection was created. In this way we could really keep track of changes in our database. 
#Create a new Database Collection, This only needs to be done once! Can be neglected in the next run
#Name of the collection = db.createCollection(name="DataFirms")


# In[34]:


#Upload Data based on Key value pairs and give it another Name
for index, row in ["INPUT DATAFRAME TO STORE"].iterrows():
    doc = ["COLLECTION CREATED IN PREVIOUS STEP"].createDocument()
    #VALUES
    doc['Firm'] = row['Company']
    if not (pd.isnull(row["inno5"])):    
        doc['Inno'] = int(row["inno5"]) #make it an integer
    if not (pd.isnull(row["Text"])):    
        doc['Text'] = row["Text"]        
    if not (pd.isnull(row["zipActivPost19"])):    
        doc['zipActivPost19'] = row["zipActivPost19"]  
    doc.save()








#Lbl2Vec Dutch

# In[1]:


#Standard packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import regex
import os
import re

#Preprocessing packages nltk
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.stem import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

#Lbl2vec model + gensim (4.1.2)
from gensim.models import Doc2Vec
from gensim.models.doc2vec import TaggedDocument
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags 
from lbl2vec import Lbl2Vec

#Metrics and prediction
from sklearn.metrics import f1_score
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score, confusion_matrix

#Connection to ArangoDB 
import getpass
import configparser
from pyArango.connection import *
from IPython.display import clear_output


# In[2]:


#Preprocess lowercase, punctuation, stopwords, lemmatization, stemming
def preprocessing(text, stopwords = None, stemming = False, lemmatization = True, cookies = None, other = None):
    #cleaning
    text = re.sub(r'[^\w\s]', " ", str(text).lower().strip())
    text = re.sub(r'\d+', " ", str(text))
    text = re.sub(r'\b\w{1,2}\b', " ", str(text))

    
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


#Tokenize words
def tokenize(document): 
    return simple_preprocess(strip_tags(document), deacc = True, min_len=2, max_len=20000)


# In[4]:


#First open the Arango application on your desktop, otherwise a connection error will occur
username = input('Please enter your username: ')
password = getpass.getpass('Please enter your password: ')
connect = Connection(username=username, password=password)
clear_output(wait=True)
print('Connected to ArangoDB')
del password 


# In[5]:


#Connect to the right Database
db = connect["ThesisDSBA"]


# In[6]:


#Connect to the right Collection
CompanyDutch = db["CompanyDutch"]


# In[7]:


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


# In[8]:

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


# In[9]:

#Languages + others
others = ['deutsch','duits','german','germany', 'dutch','nederlands','engels', 'english','francais','français','frans', 'french',
          'france','belgië','belgie','belgium','nederland','netherlands','duitsland','espanol','language', 'benelux',
          'languages','talen','taal','select','selecteer', 'selecteren', 'united','kingdom',
          'verenigd','koninkrijk', 'afspraak','maandag','dinsdag','woensdag','donderdag','vrijdag','zaterdag',
          'zondag','gesloten','openingsuren','instagram','youtube', 'linkedin', 'twitter','facebook','januari', 'februari', 'maart',
          'april','mei','juni','juli','augustus','september','oktober','november','december', 'afspraak','meeting','appointment',
          'monday','tuesday','wednesday','thursday','friday','saturday','sunday','openingsuren','opening','hours','gesloten', 
          'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'oktober', 'november', 'december', 'vlaanderen'] 


# In[10]:


#Stopwords removed from the text
stopwords_English = stopwords.words("English")   
stopwords_Dutch = stopwords.words("Dutch")
stopwords_all = stopwords_Dutch + stopwords_English


# In[11]:


#The actual preprocessing of the data
CompanyDutch["preprocessed"] = CompanyDutch["Text"].apply(lambda x:
                                              preprocessing(x, stopwords = stopwords_all, lemmatization = False, stemming = False, cookies=cookiess, other=others))


# In[12]:


#Split in training and test
labeledcompanies = pd.read_csv("/Users/victorvanhullebusch/Documents/labeling/labeled_companies.csv", sep=";")
dataset = labeledcompanies.merge(CompanyDutch, left_on='Firm', right_on='Firm', how='inner')
training_set  = pd.concat([labeledcompanies, CompanyDutch],axis=0)
training_set = training_set.drop_duplicates(subset=['Firm'],keep=False)
training_set = training_set.dropna(subset=["preprocessed"])


# In[13]:


#Set indices
indices = np.arange(0,len(dataset) , 1).tolist()
dataset["indices"] = indices
dataset = dataset.set_index('indices')
indices = np.arange(0,len(training_set) , 1).tolist()
training_set["indices"] = indices
training_set = training_set.set_index('indices')


# In[14]:


#Take necessary columns from train/test set
testdata = pd.DataFrame({"article" : dataset.preprocessed,  "Filename": dataset.Firm, "class_index": dataset.Inno, "sector": dataset.Sector})
traindata = pd.DataFrame({"article" : training_set.preprocessed, "Filename": training_set.Firm, "class_index": training_set.Inno})


# In[15]:


#Import+display labels
labels = pd.read_csv("/Users/victorvanhullebusch/Documents/keywords_lbl2vec.csv", sep=";", names=["class_name", "keywords"])
display(labels) 


# In[16]:


#The actual preprocessing of the data
labels["keywords"] = labels["keywords"].apply(lambda x:
                                              preprocessing(x, stopwords = stopwords_all, lemmatization = False, stemming = False, cookies=cookiess, other=others))

#Preprocess keywords
labels['keywords'] = labels['keywords'].apply(lambda x: x.split(' '))                                             
print(labels['keywords'])


# In[17]:


#Preprocessing 
traindata['data_set_type']= 'train'
testdata['data_set_type']= 'test'
traindata['tokens'] = traindata.apply(lambda row: TaggedDocument(tokenize(row['article']), [str(row.name)]), axis=1)
testdata['tokens'] = testdata.apply(lambda row: TaggedDocument(tokenize(row['article']), [str(row.name)]), axis=1)
traindata['doc_key'] = traindata.index.astype(str)
testdata['doc_key'] = testdata.index.astype(str)


# In[18]:

#LBL2VEC

#Train Lbl2vec
Lbl2Vec_model = Lbl2Vec(keywords_list=labels.keywords.values.tolist(), tagged_documents=traindata['tokens'], label_names=labels.class_name.astype(str).values.tolist(), similarity_threshold = 0.3,  epochs=30, min_count=2, clean_outlier = True, window=30,verbose=True)
Lbl2Vec_model.fit()


# In[19]:


#Testset
testsimilarities = Lbl2Vec_model.predict_new_docs(tagged_docs=testdata['tokens'])
evaltest = testsimilarities.merge(testdata, left_on='doc_key', right_on='doc_key')


# In[20]:


#F1
truetest = evaltest['sector'].astype(str)
predtest = evaltest['most_similar_label']
print('F1 score:')
print(f1_score(truetest, predtest, average='macro'))


# In[21]:


#Accuracy + classification report 
print('accuracy') 
print('--------') 
print(accuracy_score(truetest, predtest))
print(classification_report(truetest, predtest))








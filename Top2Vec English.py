#Top2Vec English

# In[1]:


#Connection to ArangoDB 
import getpass
import configparser
from pyArango.connection import *
from IPython.display import clear_output

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

#TOP2VEC (Gensim 3.8.3)
from top2vec import Top2Vec
from gensim.models import Phrases
from gensim.models.phrases import Phraser
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags 


#ARANGO Connection

# In[2]:


#First open the Arango application on your desktop, otherwise a connection error will occur
username = input('Please enter your username: ')
password = getpass.getpass('Please enter your password: ')
connect = Connection(username=username, password=password)
clear_output(wait=True)
print('Connected to ArangoDB')
del password 


# In[3]:


#Connect to the right Database
db = connect["ThesisDSBA"]


# In[4]:


#Connect to the right Collection
CompanyEnglish = db["CompanyEnglish"]


# In[5]:


#Get Data out of ArangoDB
Firm = []
Inno = []
Text = []
zipActivPost19 = []
for b in CompanyEnglish.fetchAll():
    Firm.append(b["Firm"])
    Inno.append(b["Inno"])
    Text.append(b["Text"])
    zipActivPost19.append(b["zipActivPost19"])
CompanyEnglish = {"Firm": Firm, "Inno": Inno, "Text": Text, "zipActivPost19": zipActivPost19 }
CompanyEnglish = pd.DataFrame(CompanyEnglish)


#Preprocessing

# In[6]:


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
        stem_ = SnowballStemmer("English")
        list_companies_text = [stem_.stem(word) for word in list_companies_text]
        
    #Lemmatization
    if lemmatization == True:
        lemmatize = WordNetLemmatizer()
        list_companies_text = [lemmatize.lemmatize(word) for word in list_companies_text]
   
        
    text = " ".join(list_companies_text)
    return text


# In[7]:


#Bigrams
def bigrams(documents):
    Sentences_to_bigrams = simple_preprocess(strip_tags(documents), deacc=True)
    return phraserBG[Sentences_to_bigrams] 


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


#Stopwords removed from the txt 
stopwords_English = stopwords.words("English")   
stopwords_Dutch = stopwords.words("Dutch")
stopwords_all = stopwords_Dutch + stopwords_English


# In[11]:


#The actual preprocessing of the data
CompanyEnglish["preprocessed"] = CompanyEnglish["Text"].apply(lambda x:
                                              preprocessing(x, stopwords = stopwords_all, lemmatization = False, stemming = False, cookies=cookiess, other=others))


# In[12]:


#Split into innovative and non-innovative firms to compare topics
noninnovative = CompanyEnglish[CompanyEnglish["Inno"] == 0.0]
innovative = CompanyEnglish[CompanyEnglish["Inno"] == 1.0]


# In[13]:


#Set indices again
indices = np.arange(0,len(innovative) , 1).tolist()
innovative["indices"] = indices
innovative = innovative.set_index('indices')
indices = np.arange(0,len(noninnovative) , 1).tolist()
noninnovative["indices"] = indices
noninnovative = noninnovative.set_index('indices')


# In[14]:


#Make tokenizer bigrams
Tokenizer = [doc.split(" ") for doc in innovative["preprocessed"].values]
bigram = Phrases(Tokenizer, min_count=5, threshold=10,  delimiter=b' ')
phraserBG = Phraser(bigram)

#INNOVATIVE

# In[15]:


#Learn the topics in TOP2VEC (+bigrams)
model_inno = Top2Vec(documents=innovative["preprocessed"].values, speed="deep-learn", tokenizer=bigrams)


# In[16]:


#Learn the topics in TOP2VEC 
model_inno = Top2Vec(documents=innovative["preprocessed"].values, speed="deep-learn")


# In[17]:


#Check number of topics
model_inno.get_num_topics()


# In[18]:


#Variables to export topics
Tword, Wscore, Tnum= model_inno.get_topics(model_inno.get_num_topics())
Tsize, Tnum = model_inno.get_topic_sizes()


# In[19]:


#Distribution of the topics
Tsize


# In[20]:


#Make matplotlib print inline
get_ipython().run_line_magic('matplotlib', 'inline')


# In[21]:


#Print topics in a wordcloud
Tword, Wscore, Tscore, Tnum = model_inno.search_topics(keywords=["innovation"], num_topics=model_inno.get_num_topics())
for T in Tnum:
    model_inno.generate_topic_wordcloud(T, background_color='white')  


# In[22]:


#Check words/topic
Tword


# In[23]:


#Variable cosine similarity/topic
Ftext, Fscore, Fid = model_inno.search_documents_by_topic(topic_num=0, num_docs=20)
for text, score, Firmid in zip(Ftext, Fscore, Fid):
    if score >= 0.0: #can vary
        print(innovative.Firm[Firmid])
    else: 
        continue


# In[24]:


#Words most related to innovation
W, Wscore= model_inno.similar_words(keywords=["innovation"], keywords_neg=[], num_words=40)
for word, score in zip(W, Wscore):
    print(f"{word} {score}") 


# In[25]:


#Variable cosine similarity/word
Ftext, Fscore, Fid = model_inno.search_documents_by_keywords(keywords=["innovation"], num_docs=100)
for  text, score, Firmid in zip(Ftext, Fscore, Fid):
    if score > 0.0: #can vary
        print(innovative.Firm[Firmid])
        print(score)
        print()
        print(text)
        print()
        print()

    else: 
        continue


# -*- coding: utf-8 -*-
"""
Created on Sat Jan 15 09:41:00 2022

@author: mario
"""

import pandas as pd
import glob
import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import dask.dataframe as dd

ANNOESTRAZIONE=0


#Classe dedicata alla creazione del grafo in Neo4j
class Neo4jData:

    def __init__(self):
        self.users = pd.DataFrame()
        self.mentioned_users = []
        self.relations = []
        # self.relations = pd.DataFrame(columns=["author", "mentioned_user", "interactions_count", "mentions_count", "quotes_count", "replies_count", "retweets_count"])
        self.users_id = set()
        self.temporal_users_dict_list=[]

     
    #Inserimento dell'autore del tweet nel dizionario con suddivisione dei likes per mensilità
    def users_temporal_likes(self, row):
        month_like=dict({"01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0, "09":0, "10":0,"11":0, "12":0})
        month_like[row[1]]=row[3]
        if row[2] == "2021":
            self.temporal_users_dict_list.append({"id":row[0],"description":row[4], "followers":row[5],"following":row[6],
                                             "total_likes_count":month_like[row[1]],
                                             "like_Jan_2021":month_like["01"],
                                             "like_Feb_2021":month_like["02"],
                                             "like_Mar_2021":month_like["03"],
                                             "like_Apr_2021":month_like["04"],
                                             "like_May_2021":month_like["05"],
                                             "like_Jun_2021":month_like["06"],
                                             "like_Jul_2021":month_like["07"],
                                             "like_Aug_2021":month_like["08"],
                                             "like_Sept_2021":month_like["09"],
                                             "like_Oct_2021":month_like["10"],
                                             "like_Nov_2021":month_like["11"],
                                             "like_Dec_2021":month_like["12"],
                                             "tweet_count":row[7],"name":row[8], "quote_count":row[9], "reply_count":row[10], "retweet_count":row[11], "username":row[12]})
        
        elif row[2] == "2020":
            self.temporal_users_dict_list.append({"id":row[0],"description":row[4], "followers":row[5],"following":row[6],
                                             "total_likes_count":month_like[row[1]],
                                             "like_Jan_2020":month_like["01"],
                                             "like_Feb_2020":month_like["02"],
                                             "like_Mar_2020":month_like["03"],
                                             "like_Apr_2020":month_like["04"],
                                             "like_May_2020":month_like["05"],
                                             "like_Jun_2020":month_like["06"],
                                             "like_Jul_2020":month_like["07"],
                                             "like_Aug_2020":month_like["08"],
                                             "like_Sept_2020":month_like["09"],
                                             "like_Oct_2020":month_like["10"],
                                             "like_Nov_2020":month_like["11"],
                                             "like_Dec_2020":month_like["12"],
                                             "tweet_count":row[7],"name":row[8], "quote_count":row[9], "reply_count":row[10], "retweet_count":row[11], "username":row[12]})
        
        
        
        
    def populate_users(self, data_frame, author_tweet_dict):
        
        #Operazioni di inizializzazione delle strutture dati
        
        self.author_tweet_dict = author_tweet_dict
        del author_tweet_dict
        
        self.users = data_frame.loc[:,[ "author.id","author.description","created_at", "author.public_metrics.followers_count", "author.public_metrics.following_count",
                                "public_metrics.like_count", "author.public_metrics.tweet_count", "author.name", "public_metrics.quote_count", "public_metrics.reply_count", 
                                "public_metrics.retweet_count", "author.username"]]
        self.users.columns = [ "id", "description","date", "followers", "following", "likes_count", "tweet_count", "name", "quote_count", "reply_count", "retweet_count", "username"]
         
        self.users["year"] = self.users["date"].str[:4]
        self.users["month"] = self.users["date"].str.slice(5,7)
        del self.users["date"]
        
        self.mention_df = data_frame.loc[:,["author.id","entities.mentions","referenced_tweets","in_reply_to_user.id","created_at"]]
        del data_frame
        self.mention_df.columns = ["id","mentions", "tweet_type","in_reply_to_user_id","date"]
        # self.mention_df.dropna(subset=["mentions"], inplace=True)
        
        self.mention_df["year"] = self.mention_df["date"].str[:4]
        self.mention_df["month"] = self.mention_df["date"].str.slice(5,7)
        del self.mention_df["date"]

        #Fine operazioni di inizializzazione        



        #Estrazione e stampa degli utenti menzionati su file csv per essere importate in Neo4j 

        print("Inizio estrazione users menzionati ed estrazioni")
        mentioned_users_numpy = self.mention_df.to_numpy()
        del self.mention_df
        vfun = np.vectorize(self.exctract_mentioned_users, signature='(n)->()')  
        vfun(mentioned_users_numpy) 
        del mentioned_users_numpy       
        print("Users menzionati estratti, inizio creazione dataframe")
        self.m_users =  pd.DataFrame.from_records(self.mentioned_users,index=["id"])
        print("Dataframe creato, inizio raggruppamento")
        del self.mentioned_users
        
        if ANNOESTRAZIONE == "2021":
            self.m_users = self.m_users.groupby("id").agg({'description':'last','followers':'last', 'following':'last',
                                                  "total_likes_count":"sum",
                                                  "like_Jan_2021":"sum",
                                                  "like_Feb_2021":"sum",
                                                  "like_Mar_2021":"sum",
                                                  "like_Apr_2021":"sum",
                                                  "like_May_2021":"sum",
                                                  "like_Jun_2021":"sum",
                                                  "like_Jul_2021":"sum",
                                                  "like_Aug_2021":"sum",
                                                  "like_Sept_2021":"sum",
                                                  "like_Oct_2021":"sum",
                                                  "like_Nov_2021":"sum",
                                                  "like_Dec_2021":"sum", 'tweet_count':'last', 'name':'last', 'quote_count':'sum', 'reply_count':'sum', 'retweet_count':'sum', 'username':'last'})
        
        elif ANNOESTRAZIONE == "2020":
            self.m_users = self.m_users.groupby("id").agg({'description':'last','followers':'last', 'following':'last',
                                                  "total_likes_count":"sum",
                                                  "like_Jan_2020":"sum",
                                                  "like_Feb_2020":"sum",
                                                  "like_Mar_2020":"sum",
                                                  "like_Apr_2020":"sum",
                                                  "like_May_2020":"sum",
                                                  "like_Jun_2020":"sum",
                                                  "like_Jul_2020":"sum",
                                                  "like_Aug_2020":"sum",
                                                  "like_Sept_2020":"sum",
                                                  "like_Oct_2020":"sum",
                                                  "like_Nov_2020":"sum",
                                                  "like_Dec_2020":"sum", 'tweet_count':'last', 'name':'last', 'quote_count':'sum', 'reply_count':'sum', 'retweet_count':'sum', 'username':'last'})
                
            
            
        print("Inizio stampa utenti menzionati")
        self.create_mentioned_users_csv()
        del self.m_users
        
        #Estrazione e stampa delle relazioni su file csv per essere importate in Neo4j 
        
        print("Inizio groupby relazioni")
        self.relations_df = dd.from_pandas(pd.DataFrame.from_records(self.relations,index=["author"]), npartitions=4)
        del self.relations
        print("Dizionario convertito, inizio groupby")
        
        if ANNOESTRAZIONE == "2021":
            self.relations_df = self.relations_df.groupby(["author","mentioned_user"]).agg({"total_interactions_count":'sum',"total_mentions_count":'sum', "total_quotes_count":'sum', "total_replies_count":'sum', "total_retweets_count":'sum',
                                    "interactions_count_Jan_2021":'sum',
                                    "interactions_count_Feb_2021":'sum',
                                    "interactions_count_Mar_2021":'sum',
                                    "interactions_count_Apr_2021":'sum',
                                    "interactions_count_May_2021":'sum',
                                    "interactions_count_Jun_2021":'sum',
                                    "interactions_count_Jul_2021":'sum',
                                    "interactions_count_Aug_2021":'sum',
                                    "interactions_count_Sept_2021":'sum',
                                    "interactions_count_Oct_2021":'sum',
                                    "interactions_count_Nov_2021":'sum',
                                    "interactions_count_Dec_2021":'sum',
                                    "mentions_count_Jan_2021":'sum',
                                    "mentions_count_Feb_2021":'sum',
                                    "mentions_count_Mar_2021":'sum',
                                    "mentions_count_Apr_2021":'sum',
                                    "mentions_count_May_2021":'sum',
                                    "mentions_count_Jun_2021":'sum',
                                    "mentions_count_Jul_2021":'sum',
                                    "mentions_count_Aug_2021":'sum',
                                    "mentions_count_Sept_2021":'sum',
                                    "mentions_count_Oct_2021":'sum',
                                    "mentions_count_Nov_2021":'sum',
                                    "mentions_count_Dec_2021":'sum',
                                    "quotes_count_Jan_2021":'sum',
                                    "quotes_count_Feb_2021":'sum',
                                    "quotes_count_Mar_2021":'sum',
                                    "quotes_count_Apr_2021":'sum',
                                    "quotes_count_May_2021":'sum',
                                    "quotes_count_Jun_2021":'sum',
                                    "quotes_count_Jul_2021":'sum',
                                    "quotes_count_Aug_2021":'sum',
                                    "quotes_count_Sept_2021":'sum',
                                    "quotes_count_Oct_2021":'sum',
                                    "quotes_count_Nov_2021":'sum',
                                    "quotes_count_Dec_2021":'sum',
                                    "replies_count_Jan_2021":'sum',
                                    "replies_count_Feb_2021":'sum',
                                    "replies_count_Mar_2021":'sum',
                                    "replies_count_Apr_2021":'sum',
                                    "replies_count_May_2021":'sum',
                                    "replies_count_Jun_2021":'sum',
                                    "replies_count_Jul_2021":'sum',
                                    "replies_count_Aug_2021":'sum',
                                    "replies_count_Sept_2021":'sum',
                                    "replies_count_Oct_2021":'sum',
                                    "replies_count_Nov_2021":'sum',
                                    "replies_count_Dec_2021":'sum',
                                    "retweets_count_Jan_2021":'sum',
                                    "retweets_count_Feb_2021":'sum',
                                    "retweets_count_Mar_2021":'sum',
                                    "retweets_count_Apr_2021":'sum',
                                    "retweets_count_May_2021":'sum',
                                    "retweets_count_Jun_2021":'sum',
                                    "retweets_count_Jul_2021":'sum',
                                    "retweets_count_Aug_2021":'sum',
                                    "retweets_count_Sept_2021":'sum',
                                    "retweets_count_Oct_2021":'sum',
                                    "retweets_count_Nov_2021":'sum',
                                    "retweets_count_Dec_2021":'sum'})
        
        elif ANNOESTRAZIONE == "2020":
            self.relations_df = self.relations_df.groupby(["author","mentioned_user"]).agg({"total_interactions_count":'sum',"total_mentions_count":'sum', "total_quotes_count":'sum', "total_replies_count":'sum', "total_retweets_count":'sum',
                                    "interactions_count_Jan_2020":'sum',
                                    "interactions_count_Feb_2020":'sum',
                                    "interactions_count_Mar_2020":'sum',
                                    "interactions_count_Apr_2020":'sum',
                                    "interactions_count_May_2020":'sum',
                                    "interactions_count_Jun_2020":'sum',
                                    "interactions_count_Jul_2020":'sum',
                                    "interactions_count_Aug_2020":'sum',
                                    "interactions_count_Sept_2020":'sum',
                                    "interactions_count_Oct_2020":'sum',
                                    "interactions_count_Nov_2020":'sum',
                                    "interactions_count_Dec_2020":'sum',
                                    "mentions_count_Jan_2020":'sum',
                                    "mentions_count_Feb_2020":'sum',
                                    "mentions_count_Mar_2020":'sum',
                                    "mentions_count_Apr_2020":'sum',
                                    "mentions_count_May_2020":'sum',
                                    "mentions_count_Jun_2020":'sum',
                                    "mentions_count_Jul_2020":'sum',
                                    "mentions_count_Aug_2020":'sum',
                                    "mentions_count_Sept_2020":'sum',
                                    "mentions_count_Oct_2020":'sum',
                                    "mentions_count_Nov_2020":'sum',
                                    "mentions_count_Dec_2020":'sum',
                                    "quotes_count_Jan_2020":'sum',
                                    "quotes_count_Feb_2020":'sum',
                                    "quotes_count_Mar_2020":'sum',
                                    "quotes_count_Apr_2020":'sum',
                                    "quotes_count_May_2020":'sum',
                                    "quotes_count_Jun_2020":'sum',
                                    "quotes_count_Jul_2020":'sum',
                                    "quotes_count_Aug_2020":'sum',
                                    "quotes_count_Sept_2020":'sum',
                                    "quotes_count_Oct_2020":'sum',
                                    "quotes_count_Nov_2020":'sum',
                                    "quotes_count_Dec_2020":'sum',
                                    "replies_count_Jan_2020":'sum',
                                    "replies_count_Feb_2020":'sum',
                                    "replies_count_Mar_2020":'sum',
                                    "replies_count_Apr_2020":'sum',
                                    "replies_count_May_2020":'sum',
                                    "replies_count_Jun_2020":'sum',
                                    "replies_count_Jul_2020":'sum',
                                    "replies_count_Aug_2020":'sum',
                                    "replies_count_Sept_2020":'sum',
                                    "replies_count_Oct_2020":'sum',
                                    "replies_count_Nov_2020":'sum',
                                    "replies_count_Dec_2020":'sum',
                                    "retweets_count_Jan_2020":'sum',
                                    "retweets_count_Feb_2020":'sum',
                                    "retweets_count_Mar_2020":'sum',
                                    "retweets_count_Apr_2020":'sum',
                                    "retweets_count_May_2020":'sum',
                                    "retweets_count_Jun_2020":'sum',
                                    "retweets_count_Jul_2020":'sum',
                                    "retweets_count_Aug_2020":'sum',
                                    "retweets_count_Sept_2020":'sum',
                                    "retweets_count_Oct_2020":'sum',
                                    "retweets_count_Nov_2020":'sum',
                                    "retweets_count_Dec_2020":'sum'})        
        
        print("Fine groupby ed inizio stampa su file")

        self.create_relations_csv()
        del self.relations_df
        
        #Estrazione e stampa degli utenti su file csv per essere importate in Neo4j 
        print("Inizio estrazione utenti")

        self.users = self.users.groupby(["id","month","year"], as_index=False).agg({'likes_count':'sum','description':'last','followers':'last', 'following':'last', 'tweet_count':'last', 'name':'last', 'quote_count':'sum', 'reply_count':'sum', 'retweet_count':'sum', 'username':'last'})
        users_numpy = np.array(self.users.to_numpy())
        del self.users
        vfun = np.vectorize(self.users_temporal_likes, signature='(n)->()')  
        vfun(users_numpy)
        del users_numpy
        print("Fine estrazione utenti ed inizio del raggruppamento")         
        self.temporal_users = pd.DataFrame.from_records(self.temporal_users_dict_list,index=["id"])
        del self.temporal_users_dict_list
        
        if ANNOESTRAZIONE == "2021":
            self.temporal_users = self.temporal_users.groupby("id").agg({"description":"last", "followers":"last","following":"last",
                                                  "total_likes_count":"sum",
                                                  "like_Jan_2021":"sum",
                                                  "like_Feb_2021":"sum",
                                                  "like_Mar_2021":"sum",
                                                  "like_Apr_2021":"sum",
                                                  "like_May_2021":"sum",
                                                  "like_Jun_2021":"sum",
                                                  "like_Jul_2021":"sum",
                                                  "like_Aug_2021":"sum",
                                                  "like_Sept_2021":"sum",
                                                  "like_Oct_2021":"sum",
                                                  "like_Nov_2021":"sum",
                                                  "like_Dec_2021":"sum",
                                                  "tweet_count":"last","name":"last", "quote_count":"last", "reply_count":"last", "retweet_count":"last", "username":"last"})
          
        elif ANNOESTRAZIONE == "2020":
            self.temporal_users = self.temporal_users.groupby("id").agg({"description":"last", "followers":"last","following":"last",
                                                  "total_likes_count":"sum",
                                                  "like_Jan_2020":"sum",
                                                  "like_Feb_2020":"sum",
                                                  "like_Mar_2020":"sum",
                                                  "like_Apr_2020":"sum",
                                                  "like_May_2020":"sum",
                                                  "like_Jun_2020":"sum",
                                                  "like_Jul_2020":"sum",
                                                  "like_Aug_2020":"sum",
                                                  "like_Sept_2020":"sum",
                                                  "like_Oct_2020":"sum",
                                                  "like_Nov_2020":"sum",
                                                  "like_Dec_2020":"sum",
                                                  "tweet_count":"last","name":"last", "quote_count":"last", "reply_count":"last", "retweet_count":"last", "username":"last"})
                    
            
            
            
        print("Inizio stampa utenti")
        self.create_temporal_users_csv()
        del self.temporal_users
        
        
        





   
    def create_temporal_users_csv(self):
        path = Path("D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/BatchInsert/"+ANNOESTRAZIONE)
        output_csv = "temporal_users.csv"
        self.temporal_users.to_csv(path / output_csv, encoding='utf-8') 
   
    def create_mentioned_users_csv(self):
        path = Path("D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/BatchInsert/"+ANNOESTRAZIONE)
        output_csv = "temporal_mentioned_users.csv"
        self.m_users.to_csv(path / output_csv, encoding='utf-8')
    
    def create_relations_csv(self):
        path = Path("D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/BatchInsert/"+ANNOESTRAZIONE)
        output_csv = "temporal_relations.csv"
        self.relations_df.to_csv(path / output_csv, encoding='utf-8', single_file=True)      

   
    def find_tweet_author(self, tweet_id):
        author = self.author_tweet_dict.get(tweet_id)
        if author:
            return str(author)
        else:
            return "NOT_FOUND"

    #Inserimento della relazione di mention con suddivisione per mensilità
    def set_mention_relation(self,author, year, month, user):
        month_like={"01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0, "09":0, "10":0,"11":0, "12":0}
        month_like[month]=1

        if year == "2021":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":1, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":0,
                                    "interactions_count_Jan_2021":month_like["01"],
                                    "interactions_count_Feb_2021":month_like["02"],
                                    "interactions_count_Mar_2021":month_like["03"],
                                    "interactions_count_Apr_2021":month_like["04"],
                                    "interactions_count_May_2021":month_like["05"],
                                    "interactions_count_Jun_2021":month_like["06"],
                                    "interactions_count_Jul_2021":month_like["07"],
                                    "interactions_count_Aug_2021":month_like["08"],
                                    "interactions_count_Sept_2021":month_like["09"],
                                    "interactions_count_Oct_2021":month_like["10"],
                                    "interactions_count_Nov_2021":month_like["11"],
                                    "interactions_count_Dec_2021":month_like["12"],
                                    "mentions_count_Jan_2021":month_like["01"],
                                    "mentions_count_Feb_2021":month_like["02"],
                                    "mentions_count_Mar_2021":month_like["03"],
                                    "mentions_count_Apr_2021":month_like["04"],
                                    "mentions_count_May_2021":month_like["05"],
                                    "mentions_count_Jun_2021":month_like["06"],
                                    "mentions_count_Jul_2021":month_like["07"],
                                    "mentions_count_Aug_2021":month_like["08"],
                                    "mentions_count_Sept_2021":month_like["09"],
                                    "mentions_count_Oct_2021":month_like["10"],
                                    "mentions_count_Nov_2021":month_like["11"],
                                    "mentions_count_Dec_2021":month_like["12"],
                                    "quotes_count_Jan_2021":0,
                                    "quotes_count_Feb_2021":0,
                                    "quotes_count_Mar_2021":0,
                                    "quotes_count_Apr_2021":0,
                                    "quotes_count_May_2021":0,
                                    "quotes_count_Jun_2021":0,
                                    "quotes_count_Jul_2021":0,
                                    "quotes_count_Aug_2021":0,
                                    "quotes_count_Sept_2021":0,
                                    "quotes_count_Oct_2021":0,
                                    "quotes_count_Nov_2021":0,
                                    "quotes_count_Dec_2021":0,
                                    "replies_count_Jan_2021":0,
                                    "replies_count_Feb_2021":0,
                                    "replies_count_Mar_2021":0,
                                    "replies_count_Apr_2021":0,
                                    "replies_count_May_2021":0,
                                    "replies_count_Jun_2021":0,
                                    "replies_count_Jul_2021":0,
                                    "replies_count_Aug_2021":0,
                                    "replies_count_Sept_2021":0,
                                    "replies_count_Oct_2021":0,
                                    "replies_count_Nov_2021":0,
                                    "replies_count_Dec_2021":0,
                                    "retweets_count_Jan_2021":0,
                                    "retweets_count_Feb_2021":0,
                                    "retweets_count_Mar_2021":0,
                                    "retweets_count_Apr_2021":0,
                                    "retweets_count_May_2021":0,
                                    "retweets_count_Jun_2021":0,
                                    "retweets_count_Jul_2021":0,
                                    "retweets_count_Aug_2021":0,
                                    "retweets_count_Sept_2021":0,
                                    "retweets_count_Oct_2021":0,
                                    "retweets_count_Nov_2021":0,
                                    "retweets_count_Dec_2021":0})
       
        elif year == "2020":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":1,
                                    "interactions_count_Jan_2020":month_like["01"],
                                    "interactions_count_Feb_2020":month_like["02"],
                                    "interactions_count_Mar_2020":month_like["03"],
                                    "interactions_count_Apr_2020":month_like["04"],
                                    "interactions_count_May_2020":month_like["05"],
                                    "interactions_count_Jun_2020":month_like["06"],
                                    "interactions_count_Jul_2020":month_like["07"],
                                    "interactions_count_Aug_2020":month_like["08"],
                                    "interactions_count_Sept_2020":month_like["09"],
                                    "interactions_count_Oct_2020":month_like["10"],
                                    "interactions_count_Nov_2020":month_like["11"],
                                    "interactions_count_Dec_2020":month_like["12"],
                                    "mentions_count_Jan_2020":month_like["01"],
                                    "mentions_count_Feb_2020":month_like["02"],
                                    "mentions_count_Mar_2020":month_like["03"],
                                    "mentions_count_Apr_2020":month_like["04"],
                                    "mentions_count_May_2020":month_like["05"],
                                    "mentions_count_Jun_2020":month_like["06"],
                                    "mentions_count_Jul_2020":month_like["07"],
                                    "mentions_count_Aug_2020":month_like["08"],
                                    "mentions_count_Sept_2020":month_like["09"],
                                    "mentions_count_Oct_2020":month_like["10"],
                                    "mentions_count_Nov_2020":month_like["11"],
                                    "mentions_count_Dec_2020":month_like["12"],
                                    "quotes_count_Jan_2020":0,
                                    "quotes_count_Feb_2020":0,
                                    "quotes_count_Mar_2020":0,
                                    "quotes_count_Apr_2020":0,
                                    "quotes_count_May_2020":0,
                                    "quotes_count_Jun_2020":0,
                                    "quotes_count_Jul_2020":0,
                                    "quotes_count_Aug_2020":0,
                                    "quotes_count_Sept_2020":0,
                                    "quotes_count_Oct_2020":0,
                                    "quotes_count_Nov_2020":0,
                                    "quotes_count_Dec_2020":0,
                                    "replies_count_Jan_2020":0,
                                    "replies_count_Feb_2020":0,
                                    "replies_count_Mar_2020":0,
                                    "replies_count_Apr_2020":0,
                                    "replies_count_May_2020":0,
                                    "replies_count_Jun_2020":0,
                                    "replies_count_Jul_2020":0,
                                    "replies_count_Aug_2020":0,
                                    "replies_count_Sept_2020":0,
                                    "replies_count_Oct_2020":0,
                                    "replies_count_Nov_2020":0,
                                    "replies_count_Dec_2020":0,
                                    "retweets_count_Jan_2020":0,
                                    "retweets_count_Feb_2020":0,
                                    "retweets_count_Mar_2020":0,
                                    "retweets_count_Apr_2020":0,
                                    "retweets_count_May_2020":0,
                                    "retweets_count_Jun_2020":0,
                                    "retweets_count_Jul_2020":0,
                                    "retweets_count_Aug_2020":0,
                                    "retweets_count_Sept_2020":0,
                                    "retweets_count_Oct_2020":0,
                                    "retweets_count_Nov_2020":0,
                                    "retweets_count_Dec_2020":0})

    #Inserimento della relazione di quote con suddivisione per mensilità
    def set_quote_relation(self,author, year, month, user):
        month_like={"01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0, "09":0, "10":0,"11":0, "12":0}
        month_like[month]=1

        if year == "2021":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":1, "total_replies_count":0, "total_retweets_count":0,
                                    "interactions_count_Jan_2021":month_like["01"],
                                    "interactions_count_Feb_2021":month_like["02"],
                                    "interactions_count_Mar_2021":month_like["03"],
                                    "interactions_count_Apr_2021":month_like["04"],
                                    "interactions_count_May_2021":month_like["05"],
                                    "interactions_count_Jun_2021":month_like["06"],
                                    "interactions_count_Jul_2021":month_like["07"],
                                    "interactions_count_Aug_2021":month_like["08"],
                                    "interactions_count_Sept_2021":month_like["09"],
                                    "interactions_count_Oct_2021":month_like["10"],
                                    "interactions_count_Nov_2021":month_like["11"],
                                    "interactions_count_Dec_2021":month_like["12"],
                                    "mentions_count_Jan_2021":0,
                                    "mentions_count_Feb_2021":0,
                                    "mentions_count_Mar_2021":0,
                                    "mentions_count_Apr_2021":0,
                                    "mentions_count_May_2021":0,
                                    "mentions_count_Jun_2021":0,
                                    "mentions_count_Jul_2021":0,
                                    "mentions_count_Aug_2021":0,
                                    "mentions_count_Sept_2021":0,
                                    "mentions_count_Oct_2021":0,
                                    "mentions_count_Nov_2021":0,
                                    "mentions_count_Dec_2021":0,
                                    "quotes_count_Jan_2021":month_like["01"],
                                    "quotes_count_Feb_2021":month_like["02"],
                                    "quotes_count_Mar_2021":month_like["03"],
                                    "quotes_count_Apr_2021":month_like["04"],
                                    "quotes_count_May_2021":month_like["05"],
                                    "quotes_count_Jun_2021":month_like["06"],
                                    "quotes_count_Jul_2021":month_like["07"],
                                    "quotes_count_Aug_2021":month_like["08"],
                                    "quotes_count_Sept_2021":month_like["09"],
                                    "quotes_count_Oct_2021":month_like["10"],
                                    "quotes_count_Nov_2021":month_like["11"],
                                    "quotes_count_Dec_2021":month_like["12"],
                                    "replies_count_Jan_2021":0,
                                    "replies_count_Feb_2021":0,
                                    "replies_count_Mar_2021":0,
                                    "replies_count_Apr_2021":0,
                                    "replies_count_May_2021":0,
                                    "replies_count_Jun_2021":0,
                                    "replies_count_Jul_2021":0,
                                    "replies_count_Aug_2021":0,
                                    "replies_count_Sept_2021":0,
                                    "replies_count_Oct_2021":0,
                                    "replies_count_Nov_2021":0,
                                    "replies_count_Dec_2021":0,
                                    "retweets_count_Jan_2021":0,
                                    "retweets_count_Feb_2021":0,
                                    "retweets_count_Mar_2021":0,
                                    "retweets_count_Apr_2021":0,
                                    "retweets_count_May_2021":0,
                                    "retweets_count_Jun_2021":0,
                                    "retweets_count_Jul_2021":0,
                                    "retweets_count_Aug_2021":0,
                                    "retweets_count_Sept_2021":0,
                                    "retweets_count_Oct_2021":0,
                                    "retweets_count_Nov_2021":0,
                                    "retweets_count_Dec_2021":0})

        elif year == "2020":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":1,
                                    "interactions_count_Jan_2020":month_like["01"],
                                    "interactions_count_Feb_2020":month_like["02"],
                                    "interactions_count_Mar_2020":month_like["03"],
                                    "interactions_count_Apr_2020":month_like["04"],
                                    "interactions_count_May_2020":month_like["05"],
                                    "interactions_count_Jun_2020":month_like["06"],
                                    "interactions_count_Jul_2020":month_like["07"],
                                    "interactions_count_Aug_2020":month_like["08"],
                                    "interactions_count_Sept_2020":month_like["09"],
                                    "interactions_count_Oct_2020":month_like["10"],
                                    "interactions_count_Nov_2020":month_like["11"],
                                    "interactions_count_Dec_2020":month_like["12"],
                                    "mentions_count_Jan_2020":0,
                                    "mentions_count_Feb_2020":0,
                                    "mentions_count_Mar_2020":0,
                                    "mentions_count_Apr_2020":0,
                                    "mentions_count_May_2020":0,
                                    "mentions_count_Jun_2020":0,
                                    "mentions_count_Jul_2020":0,
                                    "mentions_count_Aug_2020":0,
                                    "mentions_count_Sept_2020":0,
                                    "mentions_count_Oct_2020":0,
                                    "mentions_count_Nov_2020":0,
                                    "mentions_count_Dec_2020":0,
                                    "quotes_count_Jan_2020":month_like["01"],
                                    "quotes_count_Feb_2020":month_like["02"],
                                    "quotes_count_Mar_2020":month_like["03"],
                                    "quotes_count_Apr_2020":month_like["04"],
                                    "quotes_count_May_2020":month_like["05"],
                                    "quotes_count_Jun_2020":month_like["06"],
                                    "quotes_count_Jul_2020":month_like["07"],
                                    "quotes_count_Aug_2020":month_like["08"],
                                    "quotes_count_Sept_2020":month_like["09"],
                                    "quotes_count_Oct_2020":month_like["10"],
                                    "quotes_count_Nov_2020":month_like["11"],
                                    "quotes_count_Dec_2020":month_like["12"],
                                    "replies_count_Jan_2020":0,
                                    "replies_count_Feb_2020":0,
                                    "replies_count_Mar_2020":0,
                                    "replies_count_Apr_2020":0,
                                    "replies_count_May_2020":0,
                                    "replies_count_Jun_2020":0,
                                    "replies_count_Jul_2020":0,
                                    "replies_count_Aug_2020":0,
                                    "replies_count_Sept_2020":0,
                                    "replies_count_Oct_2020":0,
                                    "replies_count_Nov_2020":0,
                                    "replies_count_Dec_2020":0,
                                    "retweets_count_Jan_2020":0,
                                    "retweets_count_Feb_2020":0,
                                    "retweets_count_Mar_2020":0,
                                    "retweets_count_Apr_2020":0,
                                    "retweets_count_May_2020":0,
                                    "retweets_count_Jun_2020":0,
                                    "retweets_count_Jul_2020":0,
                                    "retweets_count_Aug_2020":0,
                                    "retweets_count_Sept_2020":0,
                                    "retweets_count_Oct_2020":0,
                                    "retweets_count_Nov_2020":0,
                                    "retweets_count_Dec_2020":0})

    #Inserimento della relazione di reply con suddivisione per mensilità
    def set_reply_relation(self,author, year, month, user):
        month_like={"01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0, "09":0, "10":0,"11":0, "12":0}
        month_like[month]=1

        if year == "2021":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":1, "total_retweets_count":0,
                                    "interactions_count_Jan_2021"+"ciao":month_like["01"],
                                    "interactions_count_Feb_2021":month_like["02"],
                                    "interactions_count_Mar_2021":month_like["03"],
                                    "interactions_count_Apr_2021":month_like["04"],
                                    "interactions_count_May_2021":month_like["05"],
                                    "interactions_count_Jun_2021":month_like["06"],
                                    "interactions_count_Jul_2021":month_like["07"],
                                    "interactions_count_Aug_2021":month_like["08"],
                                    "interactions_count_Sept_2021":month_like["09"],
                                    "interactions_count_Oct_2021":month_like["10"],
                                    "interactions_count_Nov_2021":month_like["11"],
                                    "interactions_count_Dec_2021":month_like["12"],
                                    "mentions_count_Jan_2021":0,
                                    "mentions_count_Feb_2021":0,
                                    "mentions_count_Mar_2021":0,
                                    "mentions_count_Apr_2021":0,
                                    "mentions_count_May_2021":0,
                                    "mentions_count_Jun_2021":0,
                                    "mentions_count_Jul_2021":0,
                                    "mentions_count_Aug_2021":0,
                                    "mentions_count_Sept_2021":0,
                                    "mentions_count_Oct_2021":0,
                                    "mentions_count_Nov_2021":0,
                                    "mentions_count_Dec_2021":0,
                                    "quotes_count_Jan_2021":0,
                                    "quotes_count_Feb_2021":0,
                                    "quotes_count_Mar_2021":0,
                                    "quotes_count_Apr_2021":0,
                                    "quotes_count_May_2021":0,
                                    "quotes_count_Jun_2021":0,
                                    "quotes_count_Jul_2021":0,
                                    "quotes_count_Aug_2021":0,
                                    "quotes_count_Sept_2021":0,
                                    "quotes_count_Oct_2021":0,
                                    "quotes_count_Nov_2021":0,
                                    "quotes_count_Dec_2021":0,
                                    "replies_count_Jan_2021":month_like["01"],
                                    "replies_count_Feb_2021":month_like["02"],
                                    "replies_count_Mar_2021":month_like["03"],
                                    "replies_count_Apr_2021":month_like["04"],
                                    "replies_count_May_2021":month_like["05"],
                                    "replies_count_Jun_2021":month_like["06"],
                                    "replies_count_Jul_2021":month_like["07"],
                                    "replies_count_Aug_2021":month_like["08"],
                                    "replies_count_Sept_2021":month_like["09"],
                                    "replies_count_Oct_2021":month_like["10"],
                                    "replies_count_Nov_2021":month_like["11"],
                                    "replies_count_Dec_2021":month_like["12"],
                                    "retweets_count_Jan_2021":0,
                                    "retweets_count_Feb_2021":0,
                                    "retweets_count_Mar_2021":0,
                                    "retweets_count_Apr_2021":0,
                                    "retweets_count_May_2021":0,
                                    "retweets_count_Jun_2021":0,
                                    "retweets_count_Jul_2021":0,
                                    "retweets_count_Aug_2021":0,
                                    "retweets_count_Sept_2021":0,
                                    "retweets_count_Oct_2021":0,
                                    "retweets_count_Nov_2021":0,
                                    "retweets_count_Dec_2021":0})

        elif year == "2020":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":1,
                                    "interactions_count_Jan_2020":month_like["01"],
                                    "interactions_count_Feb_2020":month_like["02"],
                                    "interactions_count_Mar_2020":month_like["03"],
                                    "interactions_count_Apr_2020":month_like["04"],
                                    "interactions_count_May_2020":month_like["05"],
                                    "interactions_count_Jun_2020":month_like["06"],
                                    "interactions_count_Jul_2020":month_like["07"],
                                    "interactions_count_Aug_2020":month_like["08"],
                                    "interactions_count_Sept_2020":month_like["09"],
                                    "interactions_count_Oct_2020":month_like["10"],
                                    "interactions_count_Nov_2020":month_like["11"],
                                    "interactions_count_Dec_2020":month_like["12"],
                                    "mentions_count_Jan_2020":0,
                                    "mentions_count_Feb_2020":0,
                                    "mentions_count_Mar_2020":0,
                                    "mentions_count_Apr_2020":0,
                                    "mentions_count_May_2020":0,
                                    "mentions_count_Jun_2020":0,
                                    "mentions_count_Jul_2020":0,
                                    "mentions_count_Aug_2020":0,
                                    "mentions_count_Sept_2020":0,
                                    "mentions_count_Oct_2020":0,
                                    "mentions_count_Nov_2020":0,
                                    "mentions_count_Dec_2020":0,
                                    "quotes_count_Jan_2020":0,
                                    "quotes_count_Feb_2020":0,
                                    "quotes_count_Mar_2020":0,
                                    "quotes_count_Apr_2020":0,
                                    "quotes_count_May_2020":0,
                                    "quotes_count_Jun_2020":0,
                                    "quotes_count_Jul_2020":0,
                                    "quotes_count_Aug_2020":0,
                                    "quotes_count_Sept_2020":0,
                                    "quotes_count_Oct_2020":0,
                                    "quotes_count_Nov_2020":0,
                                    "quotes_count_Dec_2020":0,
                                    "replies_count_Jan_2020":month_like["01"],
                                    "replies_count_Feb_2020":month_like["02"],
                                    "replies_count_Mar_2020":month_like["03"],
                                    "replies_count_Apr_2020":month_like["04"],
                                    "replies_count_May_2020":month_like["05"],
                                    "replies_count_Jun_2020":month_like["06"],
                                    "replies_count_Jul_2020":month_like["07"],
                                    "replies_count_Aug_2020":month_like["08"],
                                    "replies_count_Sept_2020":month_like["09"],
                                    "replies_count_Oct_2020":month_like["10"],
                                    "replies_count_Nov_2020":month_like["11"],
                                    "replies_count_Dec_2020":month_like["12"],
                                    "retweets_count_Jan_2020":0,
                                    "retweets_count_Feb_2020":0,
                                    "retweets_count_Mar_2020":0,
                                    "retweets_count_Apr_2020":0,
                                    "retweets_count_May_2020":0,
                                    "retweets_count_Jun_2020":0,
                                    "retweets_count_Jul_2020":0,
                                    "retweets_count_Aug_2020":0,
                                    "retweets_count_Sept_2020":0,
                                    "retweets_count_Oct_2020":0,
                                    "retweets_count_Nov_2020":0,
                                    "retweets_count_Dec_2020":0})




    #Inserimento della relazione di retweet con suddivisione per mensilità
    def set_retweet_relation(self,author, year, month, user):
        month_like={"01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0, "09":0, "10":0,"11":0, "12":0}
        month_like[month]=1

        if year == "2021":
            self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":1,
                                    "interactions_count_Jan_2021":month_like["01"],
                                    "interactions_count_Feb_2021":month_like["02"],
                                    "interactions_count_Mar_2021":month_like["03"],
                                    "interactions_count_Apr_2021":month_like["04"],
                                    "interactions_count_May_2021":month_like["05"],
                                    "interactions_count_Jun_2021":month_like["06"],
                                    "interactions_count_Jul_2021":month_like["07"],
                                    "interactions_count_Aug_2021":month_like["08"],
                                    "interactions_count_Sept_2021":month_like["09"],
                                    "interactions_count_Oct_2021":month_like["10"],
                                    "interactions_count_Nov_2021":month_like["11"],
                                    "interactions_count_Dec_2021":month_like["12"],
                                    "mentions_count_Jan_2021":0,
                                    "mentions_count_Feb_2021":0,
                                    "mentions_count_Mar_2021":0,
                                    "mentions_count_Apr_2021":0,
                                    "mentions_count_May_2021":0,
                                    "mentions_count_Jun_2021":0,
                                    "mentions_count_Jul_2021":0,
                                    "mentions_count_Aug_2021":0,
                                    "mentions_count_Sept_2021":0,
                                    "mentions_count_Oct_2021":0,
                                    "mentions_count_Nov_2021":0,
                                    "mentions_count_Dec_2021":0,
                                    "quotes_count_Jan_2021":0,
                                    "quotes_count_Feb_2021":0,
                                    "quotes_count_Mar_2021":0,
                                    "quotes_count_Apr_2021":0,
                                    "quotes_count_May_2021":0,
                                    "quotes_count_Jun_2021":0,
                                    "quotes_count_Jul_2021":0,
                                    "quotes_count_Aug_2021":0,
                                    "quotes_count_Sept_2021":0,
                                    "quotes_count_Oct_2021":0,
                                    "quotes_count_Nov_2021":0,
                                    "quotes_count_Dec_2021":0,
                                    "replies_count_Jan_2021":0,
                                    "replies_count_Feb_2021":0,
                                    "replies_count_Mar_2021":0,
                                    "replies_count_Apr_2021":0,
                                    "replies_count_May_2021":0,
                                    "replies_count_Jun_2021":0,
                                    "replies_count_Jul_2021":0,
                                    "replies_count_Aug_2021":0,
                                    "replies_count_Sept_2021":0,
                                    "replies_count_Oct_2021":0,
                                    "replies_count_Nov_2021":0,
                                    "replies_count_Dec_2021":0,
                                    "retweets_count_Jan_2021":month_like["01"],
                                    "retweets_count_Feb_2021":month_like["02"],
                                    "retweets_count_Mar_2021":month_like["03"],
                                    "retweets_count_Apr_2021":month_like["04"],
                                    "retweets_count_May_2021":month_like["05"],
                                    "retweets_count_Jun_2021":month_like["06"],
                                    "retweets_count_Jul_2021":month_like["07"],
                                    "retweets_count_Aug_2021":month_like["08"],
                                    "retweets_count_Sept_2021":month_like["09"],
                                    "retweets_count_Oct_2021":month_like["10"],
                                    "retweets_count_Nov_2021":month_like["11"],
                                    "retweets_count_Dec_2021":month_like["12"]})

        elif year == "2020":
                self.relations.append({"author":author,"mentioned_user":user, "total_interactions_count":1,"total_mentions_count":0, "total_quotes_count":0, "total_replies_count":0, "total_retweets_count":1,
                                        "interactions_count_Jan_2020":month_like["01"],
                                        "interactions_count_Feb_2020":month_like["02"],
                                        "interactions_count_Mar_2020":month_like["03"],
                                        "interactions_count_Apr_2020":month_like["04"],
                                        "interactions_count_May_2020":month_like["05"],
                                        "interactions_count_Jun_2020":month_like["06"],
                                        "interactions_count_Jul_2020":month_like["07"],
                                        "interactions_count_Aug_2020":month_like["08"],
                                        "interactions_count_Sept_2020":month_like["09"],
                                        "interactions_count_Oct_2020":month_like["10"],
                                        "interactions_count_Nov_2020":month_like["11"],
                                        "interactions_count_Dec_2020":month_like["12"],
                                        "mentions_count_Jan_2020":0,
                                        "mentions_count_Feb_2020":0,
                                        "mentions_count_Mar_2020":0,
                                        "mentions_count_Apr_2020":0,
                                        "mentions_count_May_2020":0,
                                        "mentions_count_Jun_2020":0,
                                        "mentions_count_Jul_2020":0,
                                        "mentions_count_Aug_2020":0,
                                        "mentions_count_Sept_2020":0,
                                        "mentions_count_Oct_2020":0,
                                        "mentions_count_Nov_2020":0,
                                        "mentions_count_Dec_2020":0,
                                        "quotes_count_Jan_2020":0,
                                        "quotes_count_Feb_2020":0,
                                        "quotes_count_Mar_2020":0,
                                        "quotes_count_Apr_2020":0,
                                        "quotes_count_May_2020":0,
                                        "quotes_count_Jun_2020":0,
                                        "quotes_count_Jul_2020":0,
                                        "quotes_count_Aug_2020":0,
                                        "quotes_count_Sept_2020":0,
                                        "quotes_count_Oct_2020":0,
                                        "quotes_count_Nov_2020":0,
                                        "quotes_count_Dec_2020":0,
                                        "replies_count_Jan_2020":0,
                                        "replies_count_Feb_2020":0,
                                        "replies_count_Mar_2020":0,
                                        "replies_count_Apr_2020":0,
                                        "replies_count_May_2020":0,
                                        "replies_count_Jun_2020":0,
                                        "replies_count_Jul_2020":0,
                                        "replies_count_Aug_2020":0,
                                        "replies_count_Sept_2020":0,
                                        "replies_count_Oct_2020":0,
                                        "replies_count_Nov_2020":0,
                                        "replies_count_Dec_2020":0,
                                        "retweets_count_Jan_2020":month_like["01"],
                                        "retweets_count_Feb_2020":month_like["02"],
                                        "retweets_count_Mar_2020":month_like["03"],
                                        "retweets_count_Apr_2020":month_like["04"],
                                        "retweets_count_May_2020":month_like["05"],
                                        "retweets_count_Jun_2020":month_like["06"],
                                        "retweets_count_Jul_2020":month_like["07"],
                                        "retweets_count_Aug_2020":month_like["08"],
                                        "retweets_count_Sept_2020":month_like["09"],
                                        "retweets_count_Oct_2020":month_like["10"],
                                        "retweets_count_Nov_2020":month_like["11"],
                                        "retweets_count_Dec_2020":month_like["12"]})



    #Funzione per l'estrazione degli utenti menzionati e delle relazioni
    def exctract_mentioned_users(self,row):

        if not pd.isna(row[1]):
            json_users = json.loads(row[1])
            for user_index in range(len(json_users)):
                if "name" in json_users[user_index]:
                    if row[4]=="2021":
                        self.mentioned_users.append({"id":json_users[user_index]["id"],"description":json_users[user_index]["description"], "followers":json_users[user_index]["public_metrics"]["followers_count"],"following":json_users[user_index]["public_metrics"]["following_count"],"total_likes_count":0,
                        "like_Jan_2021":0,
                        "like_Feb_2021":0,
                        "like_Mar_2021":0,
                        "like_Apr_2021":0,
                        "like_May_2021":0,
                        "like_Jun_2021":0,
                        "like_Jul_2021":0,
                        "like_Aug_2021":0,
                        "like_Sept_2021":0,
                        "like_Oct_2021":0,
                        "like_Nov_2021":0,
                        "like_Dec_2021":0, 
                        "tweet_count":json_users[user_index]["public_metrics"]["tweet_count"],"name":json_users[user_index]["name"], "quote_count":0, "reply_count":0, "retweet_count":0, "username":json_users[user_index]["username"]})
                    elif row[4]=="2020":
                        self.mentioned_users.append({"id":json_users[user_index]["id"],"description":json_users[user_index]["description"], "followers":json_users[user_index]["public_metrics"]["followers_count"],"following":json_users[user_index]["public_metrics"]["following_count"],"total_likes_count":0,
                        "like_Jan_2020":0,
                        "like_Feb_2020":0,
                        "like_Mar_2020":0,
                        "like_Apr_2020":0,
                        "like_May_2020":0,
                        "like_Jun_2020":0,
                        "like_Jul_2020":0,
                        "like_Aug_2020":0,
                        "like_Sept_2020":0,
                        "like_Oct_2020":0,
                        "like_Nov_2020":0,
                        "like_Dec_2020":0, 
                        "tweet_count":json_users[user_index]["public_metrics"]["tweet_count"],"name":json_users[user_index]["name"], "quote_count":0, "reply_count":0, "retweet_count":0, "username":json_users[user_index]["username"]})
                                      
                
                
                #CASO IN CUI IL TWEET NON E' QUOTE/RETWEET/REPLY MA MENTION, QUINDI CAMPO TWEET TYPE VUOTO
                if isinstance(row[2], (int, bool, float, type(None))) or user_index > 0:
                    self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                
                #CAMPO TWEET TYPE NON VUOTO
                else:
                    tweet_type = json.loads(row[2])
                    
                    #TROVA L'AUTORE DEL TWEET ORIGINE DELLA CONVERSAZIONE
                    original_tweet_author = self.find_tweet_author(tweet_type[0]["id"])
                    
                    #CASO DI REPLIED
                    if tweet_type[0]["type"] == "replied_to" and user_index == 0:
                        
                        #SE L'AUTORE E' DEFINITO NEL CAMPO IN_REPLY_TO_USER_ID E COINCIDE COL PRIMO UTENTE MENZIONATO
                        if json_users[user_index]["id"] == row[3] and not pd.isna(row[3]):
                            self.set_reply_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                            
                        #SE L'AUTORE E' DEFINITO NEL CAMPO IN_REPLY_TO_USER_ID E NON COINCIDE COL PRIMO UTENTE MENZIONATO
                        elif not pd.isna(row[3]):
                            self.set_reply_relation(row[0], row[4], row[5], row[3])
                            self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
                        #SE L'AUTORE DEL TWEET ORIGINALE COINCIDE COL PRIMO UTENTE MENZIONATO
                        elif json_users[user_index]["id"] == original_tweet_author:
                            self.set_reply_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                            
                        #SE L'AUTORE DEL TWEET ORIGINALE NON COINCIDE COL PRIMO UTENTE MENZIONATO    
                        elif original_tweet_author != "NOT_FOUND":
                            self.set_reply_relation(row[0], row[4], row[5], original_tweet_author)
                            self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
                        #SE IL TWEET ORIGINALE NON E' PRESENTE NEL DB E' CONSIDERATA COME MENZIONE
                        else:
                            self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
                        
                        
                    #CASO DI QUOTED    
                    elif tweet_type[0]["type"] == "quoted" and user_index == 0:
                        
                        #SE L'AUTORE DEL TWEET ORIGINALE COINCIDE COL PRIMO UTENTE MENZIONATO
                        if json_users[user_index]["id"] == original_tweet_author:
                            self.set_quote_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
                        #SE L'AUTORE DEL TWEET ORIGINALE NON COINCIDE COL PRIMO UTENTE MENZIONATO    
                        elif original_tweet_author != "NOT_FOUND":
                            self.set_quote_relation(row[0], row[4], row[5], original_tweet_author)
                            self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
                        #SE IL TWEET ORIGINALE NON E' PRESENTE NEL DB E' CONSIDERATA COME MENZIONE
                        else:
                            self.set_mention_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                    
                    #CASO DI RETWEET, IL PRIMO UTENTE MENZIONATO E' SEMPRE L'AUTORE DEL TWEET ORIGINALE        
                    elif tweet_type[0]["type"] == "retweeted" and user_index == 0:
                        self.set_retweet_relation(row[0], row[4], row[5], json_users[user_index]["id"])
                        
        #CASO IN CUI NON CI SONO UTENTI MENZIONATI MA IL TWEET E' DI TIPO REPLIED/RETWEET/QUOTED
        elif not isinstance(row[2], (int, bool, float, type(None))):
            tweet_type = json.loads(row[2])
            original_tweet_author = self.find_tweet_author(tweet_type[0]["id"])
            
            #SE L'AUTORE E' DEFINITO NEL CAMPO IN_REPLY_TO_USER_ID
            if tweet_type[0]["type"] == "replied_to" and not pd.isna(row[3]):
                self.set_reply_relation(row[0], row[4], row[5], row[3])
                
            #RICERCA DELL'AUTORE DELLA REPLY NEL TWEET ORIGINALE SE PRESENTE
            elif tweet_type[0]["type"] == "replied_to" and original_tweet_author != "NOT_FOUND":
                self.set_reply_relation(row[0], row[4], row[5], original_tweet_author)
                
            #RICERCA DELL'AUTORE DELLA QUOTE NEL TWEET ORIGINALE SE PRESENTE
            elif tweet_type[0]["type"] == "quoted" and original_tweet_author != "NOT_FOUND":
                self.set_quote_relation(row[0], row[4], row[5], original_tweet_author)
                
            #RICERCA DELL'AUTORE DEL RETWEET NEL TWEET ORIGINALE SE PRESENTE
            elif tweet_type[0]["type"] == "retweeted" and original_tweet_author != "NOT_FOUND":
                self.set_retweet_relation(row[0], row[4], row[5], original_tweet_author)
          

            
#Classe rappresentativa Dataframe dei Tweets        
class Tweets:
    def __init__(self):
        self.complete_data = pd.DataFrame()
        # self.folder_path = 'D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/DatasetUnionRT/'+ANNOESTRAZIONE+"_single_files/"
        self.folder_path = 'D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/DatasetUnionRT/'+ANNOESTRAZIONE+'/'
        self.file_list = glob.glob(self.folder_path + "*.csv")
        self.typology_counter = {"Tweets":0, "Mentions":0, "Replies":0, "Replies with mentions":0, "Quotes":0, "Quotes with mentions":0, "Retweets":0}

        
    #Lettura da tutti i csv e creazione Dataframe complessivo    
    def read_csvs(self):
        # print("Inizio lettura")  
        # for i in range(len(self.file_list)):
        #     print(self.file_list[i])
        #     data = pd.read_csv(self.file_list[i],dtype={"id":  np.int64, 					
        #                 		"created_at": pd.StringDtype(),
        #                 		"text": pd.StringDtype(),
        #                 		"attachments.media": object,
        #                 		"attachments.media_keys": object,
        #                 		"attachments.poll.duration_minutes": object,
        #                 		"attachments.poll.end_datetime": object,
        #                 		"attachments.poll.id":  np.float64,
        #                 		"attachments.poll.options": object,
        #                 		"attachments.poll.voting_status": object,
        #                 		"attachments.poll_ids": object,
        #                 		"author.id":  np.int64,
        #                 		"author.created_at": object,
        #                 		"author.username": pd.StringDtype(),
        #                 		"author.name":pd.StringDtype(),
        #                 		"author.description":pd.StringDtype(),
        #                 		"author.entities.description.cashtags": object,
        #                 		"author.entities.description.hashtags": object,
        #                 		"author.entities.description.mentions": object,
        #                 		"author.entities.description.urls": object,
        #                 		"author.entities.url.urls": object,
        #                 		"author.location": pd.StringDtype(),
        #                 		"author.pinned_tweet_id":  np.float64,
        #                 		"author.profile_image_url": pd.StringDtype(),
        #                 		"author.protected": pd.StringDtype(),
        #                 		"author.public_metrics.followers_count":  np.int64,
        #                 		"author.public_metrics.following_count":  np.int64,
        #                 		"author.public_metrics.listed_count":  np.int64,
        #                 		"author.public_metrics.tweet_count":  np.int64,
        #                 		"author.url,author.verified": pd.StringDtype(),
        #                 		"author.withheld.scope": pd.StringDtype(),
        #                 		"author.withheld.copyright": pd.StringDtype(),
        #                 		"author.withheld.country_codes": pd.StringDtype(),
        #                 		"author_id":  np.int64,
        #                 		"context_annotations": object,
        #                 		"conversation_id":  np.int64,
        #                 		"entities.annotations": object,
        #                 		"entities.cashtags": object,
        #                 		"entities.hashtags": object,
        #                 		"entities.mentions": object,
        #                 		"entities.urls": pd.StringDtype(),
        #                 		"geo.coordinates.coordinates": pd.StringDtype(),
        #                 		"geo.coordinates.type": pd.StringDtype(),
        #                 		"geo.country": pd.StringDtype(),
        #                 		"geo.country_code": object,
        #                 		"geo.full_name": pd.StringDtype(),
        #                 		"geo.geo.bbox": object,
        #                 		"geo.geo.type": pd.StringDtype(),
        #                 		"geo.id": pd.StringDtype(),
        #                 		"geo.name": pd.StringDtype(),
        #                 		"geo.place_id": pd.StringDtype(),
        #                 		"geo.place_type": pd.StringDtype(),
        #                 		"in_reply_to_user.id":  np.float64,
        #                 		"in_reply_to_user.created_at": object,
        #                 		"in_reply_to_user.username": pd.StringDtype(),
        #                 		"in_reply_to_user.name": pd.StringDtype(),
        #                 		"in_reply_to_user.description": pd.StringDtype(),
        #                 		"in_reply_to_user.entities.description.cashtags": object,
        #                 		"in_reply_to_user.entities.description.hashtags": object,
        #                 		"in_reply_to_user.entities.description.mentions": object,
        #                 		"in_reply_to_user.entities.description.urls": object,
        #                 		"in_reply_to_user.entities.url.urls": pd.StringDtype(),
        #                 		"in_reply_to_user.location": pd.StringDtype(),
        #                 		"in_reply_to_user.pinned_tweet_id":  np.float64,
        #                 		"in_reply_to_user.profile_image_url": pd.StringDtype(),
        #                 		"in_reply_to_user.protected": pd.StringDtype(),
        #                 		"in_reply_to_user.public_metrics.followers_count":  np.float64,
        #                 		"in_reply_to_user.public_metrics.following_count":  np.float64,
        #                 		"in_reply_to_user.public_metrics.listed_count":  np.float64,
        #                 		"in_reply_to_user.public_metrics.tweet_count":  np.float64,
        #                 		"in_reply_to_user.url": pd.StringDtype(),
        #                 		"in_reply_to_user.verified": pd.StringDtype(),
        #                 		"in_reply_to_user.withheld.scope": pd.StringDtype(),
        #                 		"in_reply_to_user.withheld.copyright": pd.StringDtype(),
        #                 		"in_reply_to_user.withheld.country_codes": object,
        #                 		"in_reply_to_user_id":  np.float64,
        #                 		"lang": pd.StringDtype(),
        #                 		"possibly_sensitive": pd.StringDtype(),
        #                 		"public_metrics.like_count":  np.int64,
        #                 		"public_metrics.quote_count":  np.int64,
        #                 		"public_metrics.reply_count":  np.int64,
        #                 		"public_metrics.retweet_count": np.int64,
        #                 		"referenced_tweets": object,
        #                 		"reply_settings": object,
        #                 		"source": pd.StringDtype(),
        #                 		"withheld.scope": pd.StringDtype(),
        #                 		"withheld.copyright": pd.StringDtype(),
        #                 		"withheld.country_codes": pd.StringDtype(),
        #                 		"type": pd.StringDtype(),
        #                 		"__twarc.retrieved_at": object,
        #                 		"__twarc.url": pd.StringDtype(),
        #                 		"__twarc.version": pd.StringDtype()
        #                  		 },
        #                         usecols=[ "id","author.id","created_at","author.description", "author.public_metrics.followers_count", "author.public_metrics.following_count",
        #                                                   "public_metrics.like_count", "author.public_metrics.tweet_count", "author.name", "public_metrics.quote_count", "public_metrics.reply_count", 
        #                                                   "public_metrics.retweet_count", "author.username","entities.mentions","referenced_tweets","in_reply_to_user.id","entities.hashtags","lang"])

        #     self.complete_data = pd.concat([self.complete_data, data], ignore_index=True)
            

        # self.complete_data.reset_index(drop=True, inplace=True)
        # self.complete_data = self.complete_data.loc[self.complete_data['lang'] == 'it']
        # print("Fine lettura")
        # print("Inizio scrittura csv")
        # self.folder_path = 'D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/DatasetUnionRT/'+ANNOESTRAZIONE+'/'
        # self.complete_data.to_csv(self.folder_path+"union.csv")
        
        # print("Fine scrittura csv")
        # print("Inizio scrittura parquet")
        # self.complete_data.to_parquet(self.folder_path+"union.parquet", compression=None, engine='fastparquet')
        # print("Fine scrittura parquet")
        

        
        #Se File Union Csv è presente
        print("Lettura csv")
        self.complete_data = pd.read_csv(self.folder_path+"union.csv",dtype={"id": np.int64, 					
                    		"author.id": np.int64,
                            "created_at": pd.StringDtype(),
                    		"author.description": pd.StringDtype(),
                    		"author.public_metrics.followers_count": np.int64,
                    		"author.public_metrics.following_count": np.int64,
                    		"public_metrics.like_count":  np.int64,
                    		"author.public_metrics.tweet_count": np.int64,
                    		"author.name": pd.StringDtype(),
                    		"public_metrics.quote_count": np.int64,
                    		"public_metrics.reply_count":  np.int64,
                    		"public_metrics.retweet_count": np.int64,
                    		"author.username": pd.StringDtype(),
                            "entities.mentions": object,
                            "referenced_tweets": object,
                            "in_reply_to_user.id": np.float64,
                            "entities.hashtags": object}, index_col=[0])
        
        del self.complete_data["lang"]

        # print("Scrittura parquet")
        # self.complete_data.to_parquet(self.folder_path+"union.parquet", compression=None, engine='fastparquet', index=False)
        # print("Inizio lettura da parquet")
        # self.complete_data = pd.read_parquet(self.folder_path+"union.parquet", engine="fastparquet")
        # # #Creo gli indici dell'intero dataframe
        # self.complete_data.reset_index(drop=True, inplace=True)
        
        print("Lettura completata")
        
        #Creazione di dizionari per migliorare l'efficenza della ricerca
        
        self.tweet_author_dict = self.complete_data.loc[:,["id","author.id"]]
        self.tweet_author_dict.columns = ["id","author"]
        self.tweet_author_dict = pd.Series(self.tweet_author_dict.author.values,index=self.tweet_author_dict.id).to_dict()
   
        self.username_name_dict = self.complete_data.loc[:,["author.username","author.name"]]
        self.username_name_dict.columns = ["username","name"]
        self.username_name_dict = pd.Series(self.username_name_dict.name.values,index=self.username_name_dict.username).to_dict()
        
        
        self.username_tweettype = self.complete_data.loc[:,["author.username","referenced_tweets"]]
        self.username_tweettype.columns = ["username", "referenced_tweets"]
        self.username_tweettype = pd.Series(self.username_tweettype.referenced_tweets.values,index=self.username_tweettype.username).groupby(level=0).agg(list).to_dict()
        
        
    #Creazione crosstab per l'istogramma degli utenti più attivi    
    def create_crosstab_data(self, best_users):
        users = []
        types = []
        for user in best_users.index:
            tweet_type = self.username_tweettype[user]
            for elem in tweet_type:
                if isinstance(elem, (int, bool, float, type(None))):
                    users.append(user)
                    types.append("tweet")
                
                else:
                    elem = json.loads(elem)
                    users.append(user)
                    types.append(elem[0]["type"])
            
        crosstab = pd.crosstab(pd.Series(users), pd.Series(types).sort_values(ascending=False), rownames=['Utenti'], colnames=['Tipi'])
        crosstab = crosstab.loc[crosstab.sum(axis=1).sort_values(axis=0, ascending=False).index]

        return crosstab
            
            
    #Metodo per l'estrazione degli hashtag, omogeneizzazione in lowercase        
    def extract_hash_tags(self):
        self.hashtags_list = []
        for single_tweet_tags_list in self.complete_data['entities.hashtags'].values:
            if not pd.isna(single_tweet_tags_list):
                single_tweet_tags_list = json.loads(single_tweet_tags_list)
                for tag in single_tweet_tags_list:
                    self.hashtags_list.append(tag['tag'].lower())


    #Ritorna il numero di hashtags
    def count_hash_tags(self):
        self.extract_hash_tags()
        series_hashtag = pd.Series(self.hashtags_list)
        return series_hashtag.value_counts()
    
    #Ritorna il numero di hashtags
    def count_user_publications(self):
        series_username = pd.Series(self.complete_data["author.username"])
        count_occurrences = series_username.value_counts()
        return count_occurrences
        # #Estraggo i nomi corrispondenti agli username
        # keys = count_occurrences.keys()
        # name_indexes = []
        # for i in range(len(keys)):
        #     name_indexes.append(self.username_name_dict.get(keys[i]))
        
        # return count_occurrences, name_indexes 
    
    #Funzione di supporto per la ricerca dell'autore di un tweet
    def find_tweet_author(self, tweet_id):
        author = self.tweet_author_dict.get(tweet_id)
        if author:
            return str(author)
        else:
            return "NOT_FOUND"
    
    #Funzione per il conteggio e suddivisione dei tweet per tipologia
    def tweet_typology_distribution(self, row):
        tweet_type=row[0]
        
        if not pd.isna(tweet_type):
            
            tweet_type = json.loads(tweet_type)
            
            if pd.isna(row[1]):
                
                if tweet_type[0]["type"] == "replied_to": 
                    self.typology_counter["Replies"]+=1
                
                elif tweet_type[0]["type"] == "quoted":
                    self.typology_counter["Quotes"]+=1
                    
                elif tweet_type[0]["type"] == "retweeted":
                    self.typology_counter["Quotes"]+=1
                else:
                    print("ERRORE PRIMO IF", row)
            
            else:
                
                number_of_mentions = row[1].count("username")
                                    
                original_author_times_mentioned = row[1].count(self.find_tweet_author(int(tweet_type[0]["id"])))
                
                if tweet_type[0]["type"] == "replied_to" and number_of_mentions==1 and original_author_times_mentioned==1:
                    self.typology_counter["Replies"]+=1
                    
                elif tweet_type[0]["type"] == "replied_to"  and number_of_mentions>1 :
                    self.typology_counter["Replies with mentions"]+=1
                
                elif tweet_type[0]["type"] == "replied_to" and number_of_mentions==1 and original_author_times_mentioned==0:
                    self.typology_counter["Replies with mentions"]+=1
                
                elif tweet_type[0]["type"] == "replied_to":
                    self.typology_counter["Replies"]+=1
                    
                
                elif tweet_type[0]["type"] == "quoted" and original_author_times_mentioned==1 and number_of_mentions==1: 
                    self.typology_counter["Quotes"]+=1
                
                elif tweet_type[0]["type"] == "quoted" and number_of_mentions>1:
                    self.typology_counter["Quotes with mentions"]+=1
                
                elif tweet_type[0]["type"] == "quoted" and number_of_mentions==1 and original_author_times_mentioned==0:
                    self.typology_counter["Quotes with mentions"]+=1
                
                elif tweet_type[0]["type"] == "quoted":
                    self.typology_counter["Quotes"]+=1
                
                elif tweet_type[0]["type"] == "retweeted":
                        self.typology_counter["Retweets"]+=1
                else:
                    print("GENERALE", row)
            
        elif not pd.isna(row[1]):
            self.typology_counter["Mentions"]+=1
        
        else:
            self.typology_counter["Tweets"]+=1

    #Funzione di supporto per la ricerca dell'autore di un tweet
    def count_monthly_tweet(self):
        tweets_con_data = self.complete_data.loc[:,["created_at"]]
        tweets_con_data["created_at"] = tweets_con_data["created_at"].str[:4] + "/" + tweets_con_data["created_at"].str.slice(5,7)
        series_n_tweets = pd.Series(tweets_con_data["created_at"])
        count_occurrences = series_n_tweets.value_counts()
        path = Path('D:/mario/Documents/Università/Progetto Tesi Magistrale/Dati/DatasetUnionRT/'+ANNOESTRAZIONE)
        output_csv = "count_occurrences.csv"
        count_occurrences.to_csv(path / output_csv, encoding='utf-8') 
        
    

#COME PRIMA COSA DECIDERE L'ANNO DI ESTRAZIONE DEI TWEET
ANNOESTRAZIONE="2021"
    
print("Inizio costruzione dataset")
mydata=Tweets()
mydata.read_csvs()
print("Dataset costruito")

# print("Inizio creazione dati per Neo4j")
# csv_creator = Neo4jData()
# csv_creator.populate_users(mydata.complete_data, mydata.tweet_author_dict)
# print("Fine creazione dati per Neo4j")

# graph_connection = CreateGraph("bolt://localhost:7687/neo4j", "neo4j", "luca")
# graph_connection.populate_users(mydata.complete_data)
# graph_connection.close()

# DISTRIBUZIONI

#Conteggio tweet per mensilità
# mydata.count_monthly_tweet()




# #Distribuzione tipologie di Tweets
topology = mydata.complete_data.loc[:,["referenced_tweets","entities.mentions"]]
topology_numpy = np.array(topology.to_numpy())
vfun = np.vectorize(mydata.tweet_typology_distribution, signature='(n)->()')  
vfun(topology_numpy)     
results = pd.Series(mydata.typology_counter)

print(results)

labels=results.keys()


# fig, ax = plt.subplots()
# ax.axis('equal')

# color_palette_list = ['#2a9ae0', '#784aa6', '#f754a6', '#fa78b9',   
#                       '#83f26f', '#b1f495', '#F8D210']
# pie = ax.pie(results, colors = color_palette_list,
#           radius = 1.8, textprops = {"fontsize":9}, explode=(0.1,0.1,0.1,0.1,0.1,0.1,0.1), autopct='%.1f%%', pctdistance=1.15)

# plt.setp(pie[1], rotation_mode="anchor", ha="center", va="center")
# #plt.legend(labels=[f'{x} {np.round(y/results.sum()*100,1)}%' for x,y in results.items()], bbox_to_anchor=(1,1), loc="lower left")
# #plt.legend(labels=[f'{x} {np.round(y/results.sum()*100,1)}%' for x,y in results.items()], bbox_to_anchor=(1,1), loc="lower left")
# plt.show()



# # Distribuzione hashtag
# hashtags_distribution = mydata.count_hash_tags()
# plt.rcParams['figure.figsize'] = (18, 10)
# hashtags_distribution[:30].plot(kind='bar',color = 'orange', alpha = 0.7, linewidth=4, edgecolor= 'red')
# plt.xlabel("Hashtags", fontsize=20)
# plt.ylabel("Occorrenze", fontsize=20)
# plt.title("I 30 hashtag con più utilizzati", fontsize=22)
# plt.xticks(rotation=90, fontsize = 13) 
# plt.show()



# #Distribuzione attività utenti
# # users_distribution, users_index = mydata.count_user_publications()
# users_distribution = mydata.count_user_publications()
# #Sostituisco gli username con i nomi
# # users_distribution.index = users_index
# plt.rcParams['figure.figsize'] = (18, 10)
# users_distribution[:30].plot(kind='bar',color = 'orange',  alpha = 0.7, linewidth=4, edgecolor= 'red')
# plt.xlabel("Utenti", fontsize=20)
# plt.ylabel("Pubblicazioni", fontsize=20)
# plt.title("I 30 utenti più attivi", fontsize=22)
# plt.xticks(rotation=90, fontsize = 13) 
# plt.show()




# #Distribuzione utenti più attivi con differenziazione tipologie di tweet
# users_distribution = mydata.count_user_publications()

# crosstab = mydata.create_crosstab_data(users_distribution[:30])
# # Creating barplot
# color_palette_list = ['#83f26f','#f754a6','#F8D210','#2a9ae0']
# crosstab.plot(kind='bar', stacked=True, figsize=(18,10), color=color_palette_list)
# plt.title("I 30 utenti più attivi", fontsize = 20)
# plt.xlabel("Utenti", fontsize=15)
# plt.ylabel("Numero Tweets Pubblicati", fontsize=15)
# plt.show()



# CONTROLLO TIPOLOGIE TWEET
# tipi = set()
# ref_series=mydata.complete_data["referenced_tweets"]
# for i in ref_series:
#     if not pd.isna(i):
#         i = json.loads(i)
#         for tag in i:
#             tipi.add(tag["type"])
# print("I tipi di tweet oltre al classico tweet e le mentions sono: ",tipi)


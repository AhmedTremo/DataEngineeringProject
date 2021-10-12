import tweepy
import os
import sys
import geocoder
from textblob import TextBlob
import requests
from airflow import DAG
from datetime import datetime
from datetime import date
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import csv
import logging
from pandas.errors import EmptyDataError

# API Keys and Tokens

consumer_key = 'gHIcdpribtGMTLjmRp8GTCXl5'
consumer_secret = 'Fw8ju1j1pv4G1s5eEezTSJ2Ep88R2UaR3SLE8giBamfT5kfPMM'
access_token ='558144474-F3lu5GErq3ws5MRJft1YdcxRrSXlIAXNhdxnlzed'
access_token_secret = 'JVEfdbsLUVy9cNBWPBsIONrZzAaJ4sZfGwSKCTLXYI1x4'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)



default_args = {
    'owner': 'airflow',
    'depends_on_past': True
    }

def get_last_dag_run(dag):
    last_dag_run = dag.get_last_dagrun()
    if last_dag_run is None:
        return datetime(2020, 12, 30)
    else:
        return last_dag_run.execution_date


# step 3 - instantiate DAG
dag = DAG(
    'Sentiment_Analysis_t3',
    default_args=default_args,
    catchup= True,
    description='Sentiment Analysis',
    start_date =datetime(2020, 12, 30),
    schedule_interval='@daily',
    user_defined_macros={
          'last_dag_run_execution_date': get_last_dag_run
      }
)

date = get_last_dag_run(dag)
next_date = date + dt.timedelta(days=1)
next_date=next_date.strftime("%Y-%m-%d")
date = date.strftime("%Y-%m-%d")

def store_data(**context):
    f = open("Result_dataset/Sentiment analysis.txt","a") 

    try:
        df = pd.read_csv("Result_dataset/Average.csv")
    except EmptyDataError:
        df = pd.DataFrame(columns=["Country","Average"," Time","Written at"])
 

    Togo_Average,t_d = context['task_instance'].xcom_pull(task_ids='Togo_analysis')
    Switzerland_Average,s_d = context['task_instance'].xcom_pull(task_ids='Switzerland_analysis')

    f.write("Togo Sentiment Average = ")
    f.write(str(Togo_Average))
    f.write("                    Written at : ")
    f.write(str(datetime.now()))
    f.write("     date = ")
    f.write(str(t_d))
    f.write("\n")
    f.write("Switzerland Sentiment Average = ")
    f.write(str(Switzerland_Average))
    f.write("             Written at : ")
    f.write(str(datetime.now()))
    f.write("     date = ")
    f.write(str(t_d))
    f.write("\n")

    Togo_df = {'Country':'TOGO', 'Average':Togo_Average, 'Time':t_d, 'Written_at':str(datetime.now())}
    switzerland_df = {'Country':'switzerland', 'Average':Switzerland_Average, 'Time':s_d, 'Written_at':str(datetime.now())}
    
    df.append(Togo_df,ignore_index=True)
    df.append(switzerland_df,ignore_index=True)

    df.to_csv("Result_dataset/Average.csv")


    
def Switzerland_analysis(**kwargs):

    noOfSearch = 5
    searchCountry = "Switzerland"
   
    places = api.geo_search(query=searchCountry, granularity="country")
    place_id = places[0].id

    tweets = tweepy.Cursor(api.search , q='place:{}'.format(place_id),since=date,until=next_date,lang="en").items(noOfSearch)
    Switzerland_Sum = 0
    for tweet in tweets:
        analysis = TextBlob(tweet.text).sentiment
        Switzerland_Sum += analysis.polarity

    return Switzerland_Sum/noOfSearch,date

        
def Togo_analysis(**kwargs):

    noOfSearch = 5
    searchCountry = "Togo"

    places = api.geo_search(query=searchCountry, granularity="country")
    place_id = places[0].id

    tweets = tweepy.Cursor(api.search , q='place:{}'.format(place_id),since=date,until=next_date,lang="en").items(noOfSearch)

    Togo_Sum = 0

    for tweet in tweets:
        analysis = TextBlob(tweet.text).sentiment
        Togo_Sum += analysis.polarity
        
    return Togo_Sum/noOfSearch,date


def extract_data_2019(**kwargs):
    df_Year_2019 =  pd.read_csv('datasets/2019.csv')
    switzerland_Happiness_Score = df_Year_2019[df_Year_2019["Country or region"] == "Switzerland"]["Score"].iloc[0]
    togo_Happiness_Score = df_Year_2019[df_Year_2019["Country or region"] == "Togo"]["Score"].iloc[0]
    return togo_Happiness_Score,switzerland_Happiness_Score

# def compare_data(**context):
#     text = ""
#     togo_Happiness_Score,switzerland_Happiness_Score = context['task_instance'].xcom_pull(task_ids='extract_data_2019')
#     df_average_data =  pd.read_csv('Result_dataset/Average.csv')

#     if(switzerland_Happiness_Score>togo_Happiness_Score):
#         text+= "In 2019 dataset Switzerland Happiness score is higher than that of Togo."
#     else:
#         text+= "In 2019 dataset Togo Happiness score is higher than that of Switzerland."
#     if()


t1 = PythonOperator(
    task_id='Togo_analysis',
    provide_context=True,
    python_callable=Togo_analysis,
    dag=dag,
)
t2 = PythonOperator(
    task_id='Switzerland_analysis',
    provide_context=True,
    python_callable=Switzerland_analysis,
    dag=dag,
)
t3 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)
t4 = PythonOperator(
    task_id='extract_data_2019',
    provide_context=True,
    python_callable=extract_data_2019,
    dag=dag,
)
extract_data_2019
t1 >> t2 >> t3 >> t4
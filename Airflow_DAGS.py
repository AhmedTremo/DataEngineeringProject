import requests
import json
from airflow import DAG
from datetime import datetime
from datetime import date
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.impute import SimpleImputer
import os 

# ResultDir = 'home/mostafa/DE_poject/project-milestone-1-m3ak-data/data/' 
# os.system('cd ' + ResultDir)

# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 13)
    }


# step 3 - instantiate DAG
dag = DAG(
    'Data_Cleaning',
    default_args=default_args,
    description='Cleaning_data',
    schedule_interval='@once',
)

# step 4 Define tasks
def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='Feature_eng')
    df.to_csv("Result_dataset/cleaned_data.csv")


def extract_data_250_country(**kwargs):
    df_Country_data = pd.read_csv('datasets/250 Country Data.csv')
    del df_Country_data["Unnamed: 0"]
    df_Country_data = df_Country_data.rename(columns = {'name': 'Country'}, inplace = False)
    return df_Country_data

def extract_data_Life_exp(**kwargs):
    df_LifeExpec = pd.read_csv('datasets/Life Expectancy Data.csv')
    df_2015_lifeExp=df_LifeExpec.loc[df_LifeExpec['Year'] == 2015]
    df_2000_lifeExp=df_LifeExpec.loc[df_LifeExpec['Year'] == 2000]
    df_2000_lifeExp_Country_LifeExpec = df_2000_lifeExp[['Country', 'Life expectancy ']]
    df_2000_lifeExp_Country_LifeExpec = df_2000_lifeExp_Country_LifeExpec.rename(columns={"Life expectancy ":"Life expectancy 2000"})
    return df_2015_lifeExp , df_2000_lifeExp_Country_LifeExpec

def extract_data_2015(**kwargs):
    df_Year_2015 =  pd.read_csv('datasets/2015.csv')
    return df_Year_2015
    
def extract_data_2019(**kwargs):
    df_Year_2019 =  pd.read_csv('datasets/2019.csv')
    df_Year_2019_Country_Score = df_Year_2019[['Country or region', 'Score']]
    df_Year_2019_Country_Score = df_Year_2019_Country_Score.rename(columns={"Country or region":"Country"})
    return df_Year_2019_Country_Score

def extract_data_GDP(**kwargs):
    df_GDP_per_capita = pd.read_csv("datasets/GDP per capita.csv")
    df_GDP_per_capita = df_GDP_per_capita[["Country Name", "2015"]]
    df_GDP_per_capita = df_GDP_per_capita.rename(columns={"Country Name":"Country"})
    return df_GDP_per_capita

def merge_data(**context):
    df_Country_data = context['task_instance'].xcom_pull(task_ids='extract_data_250')

    df_LifeExpec,df_2000_lifeExp_Country_LifeExpec = context['task_instance'].xcom_pull(task_ids='extract_data_LE')

    df_Year_2015 = context['task_instance'].xcom_pull(task_ids='extract_data_2015')

    df_Year_2019_Country_Score = context['task_instance'].xcom_pull(task_ids='extract_data_2019')


    df_merge_2015_Exp = pd.merge(df_Year_2015,df_LifeExpec, on='Country')

    df_merge_2015_Exp_250Country = pd.merge(df_merge_2015_Exp,df_Country_data , on='Country')

    df_merge_2015_Exp_250Country = pd.merge(df_merge_2015_Exp_250Country,df_Year_2019_Country_Score, on='Country', how='left')

    df_merge_2015_Exp_250Country = pd.merge(df_merge_2015_Exp_250Country,df_2000_lifeExp_Country_LifeExpec, on='Country', how='left')
    
    return df_merge_2015_Exp_250Country

def data_cleaning(**context):
    df_merge_2015_Exp_250Country = context['task_instance'].xcom_pull(task_ids='merge_data')

    df_merge_2015_Exp_250Country = df_merge_2015_Exp_250Country.rename(columns={"Score":"Happiness Score 2019"})

    df_merge_2015_Exp_250Country['Happiness Score 2019'] = df_merge_2015_Exp_250Country['Happiness Score 2019'].fillna(df_merge_2015_Exp_250Country['Happiness Score 2019'].mean())

    unemployment = df_merge_2015_Exp_250Country.loc[:,'Unemployement(%)':'Unemployement(%)']

    unemployment['Unemployement(%)'].replace({'N.A.':np.nan,'n.a.':np.nan,'NaN':np.nan},inplace=True)
    
    unemployment['Unemployement(%)'] = unemployment['Unemployement(%)'].str.extract('(\d+)').astype(float)

    NaN_Values = unemployment['Unemployement(%)'].isna().sum()


    df_merge_2015_Exp_250Country['Unemployement(%)'] =unemployment['Unemployement(%)']


    grouped_by_region = df_merge_2015_Exp_250Country.groupby(['Region'])["Unemployement(%)"].mean()


    for i in range(0,10):
        region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Unemployement(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==grouped_by_region.index[i])]
        region_nan_df['Unemployement(%)'] = grouped_by_region.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Unemployement(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==grouped_by_region.index[i])]=region_nan_df


    df_Literacy_Rate = df_merge_2015_Exp_250Country.loc[:,'Literacy Rate(%)':'Literacy Rate(%)']



    df_Literacy_Rate['Literacy Rate(%)'] = df_Literacy_Rate['Literacy Rate(%)'].str.extract('(\d+)').astype(float)


    df_Literacy_Rate_list = df_Literacy_Rate['Literacy Rate(%)'].sort_values(ascending=True).values.tolist()


    df_merge_2015_Exp_250Country["Literacy Rate(%)"] = df_Literacy_Rate['Literacy Rate(%)']


    lit_grouped_by_region_dev = df_merge_2015_Exp_250Country.groupby(['Region','Status'])["Literacy Rate(%)"].median()

    for i in range(0,14):
        lit_region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Literacy Rate(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==lit_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==lit_grouped_by_region_dev.index[i][1])]
        lit_region_nan_df['Literacy Rate(%)'] = lit_grouped_by_region_dev.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Literacy Rate(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==lit_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==lit_grouped_by_region_dev.index[i][1])]=lit_region_nan_df


    df_Real_Growth_Rating = df_merge_2015_Exp_250Country.loc[:,'Real Growth Rating(%)':'Real Growth Rating(%)']


    df_Real_Growth_Rating['Real Growth Rating(%)'] = df_Real_Growth_Rating['Real Growth Rating(%)'].str.extract('(\d+)').astype(float)


    df_growth_Rate_list = df_Real_Growth_Rating['Real Growth Rating(%)'] .sort_values(ascending=True).values.tolist()

    df_merge_2015_Exp_250Country["Real Growth Rating(%)"] = df_Real_Growth_Rating['Real Growth Rating(%)']


    growth_grouped_by_region_dev = df_merge_2015_Exp_250Country.groupby(['Region','Status'])["Real Growth Rating(%)"].median()


    for i in range(0,14):
        growth_region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Real Growth Rating(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==growth_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==growth_grouped_by_region_dev.index[i][1])]
        growth_region_nan_df['Real Growth Rating(%)'] = growth_grouped_by_region_dev.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Real Growth Rating(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==growth_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==growth_grouped_by_region_dev.index[i][1])]=growth_region_nan_df


    df_Inflation = df_merge_2015_Exp_250Country.loc[:,'Inflation(%)':'Inflation(%)']


    df_Inflation['Inflation(%)'] = df_Inflation['Inflation(%)'].str.extract('([-\d\.]+)').astype(float)


    df_Inflation_list = df_Inflation['Inflation(%)'] .sort_values(ascending=True).values.tolist()



    df_merge_2015_Exp_250Country['Inflation(%)'] = df_Inflation['Inflation(%)'] 


    Inf_grouped_by_region_dev = df_merge_2015_Exp_250Country.groupby(['Region','Status'])['Inflation(%)'].mean()


    for i in range(0,14):
        inf_region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Inflation(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==Inf_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==Inf_grouped_by_region_dev.index[i][1])]
        inf_region_nan_df['Inflation(%)'] = Inf_grouped_by_region_dev.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Inflation(%)'].isna()) & (df_merge_2015_Exp_250Country['Region']==Inf_grouped_by_region_dev.index[i][0])& (df_merge_2015_Exp_250Country['Status']==Inf_grouped_by_region_dev.index[i][1])]=inf_region_nan_df


    df_merge_2015_Exp_250Country = df_merge_2015_Exp_250Country.drop(["Alcohol", "Total expenditure", "Population","region","subregion"], axis=1)

    df_merge_2015_Exp_250Country = df_merge_2015_Exp_250Country[df_merge_2015_Exp_250Country['Region'] != "North America"]


    gini_grouped_by_region_status = df_merge_2015_Exp_250Country.groupby(['Region','Status'])['gini'].mean()


    for i in range(0,13):
        gini_region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['gini'].isna()) & (df_merge_2015_Exp_250Country['Region']==gini_grouped_by_region_status.index[i][0])& (df_merge_2015_Exp_250Country['Status']==gini_grouped_by_region_status.index[i][1])]
        gini_region_nan_df['gini'] = gini_grouped_by_region_status.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['gini'].isna()) & (df_merge_2015_Exp_250Country['Region']==gini_grouped_by_region_status.index[i][0])& (df_merge_2015_Exp_250Country['Status']==gini_grouped_by_region_status.index[i][1])]=gini_region_nan_df


    df_missing_gdp = df_merge_2015_Exp_250Country[["GDP", "Literacy Rate(%)", "Inflation(%)", "Unemployement(%)"]]


    df_GDP_per_capita = context['task_instance'].xcom_pull(task_ids='extract_data_GDP')

    df_merge_2015_Exp_250Country = pd.merge(df_merge_2015_Exp_250Country,df_GDP_per_capita , on='Country')
    df_merge_2015_Exp_250Country

    df_merge_2015_Exp_250Country.drop(["GDP"],axis=1,inplace=True)

    df_merge_2015_Exp_250Country.rename(columns={"2015":"GDP"}, inplace=True)
    df_merge_2015_Exp_250Country["GDP"] = pd.to_numeric(df_merge_2015_Exp_250Country["GDP"])
    



    df_hieghest_thiness = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Region']=='Sub-Saharan Africa')]



    Thiness1_19mean=df_hieghest_thiness[' thinness  1-19 years'].mean()
    Thiness5_9mean =df_hieghest_thiness[' thinness 5-9 years'].mean()
    df_merge_2015_Exp_250Country[' thinness  1-19 years'].loc[(df_merge_2015_Exp_250Country['Country'] == "Sudan")]=Thiness1_19mean
    df_merge_2015_Exp_250Country[' thinness 5-9 years'].loc[(df_merge_2015_Exp_250Country['Country'] == "Sudan")]=Thiness5_9mean
    df_merge_2015_Exp_250Country[df_merge_2015_Exp_250Country['Country']=="Sudan"]

    df_Region_Bmi = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Region']=='Sub-Saharan Africa')]


    Region_BMI_mean=df_Region_Bmi[' BMI '].mean()

    df_merge_2015_Exp_250Country[' BMI '].loc[(df_merge_2015_Exp_250Country['Country'] == "Sudan")]=Region_BMI_mean
    df_merge_2015_Exp_250Country[df_merge_2015_Exp_250Country['Country']=="Sudan"]


    df_Hepatitis = df_merge_2015_Exp_250Country.loc[:,'Hepatitis B':'Hepatitis B']



    Hep_grouped_by_region_status = df_merge_2015_Exp_250Country.groupby(['Region'])['Hepatitis B'].median()


    for i in range(0,9):
        Hep_region_nan_df = df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Hepatitis B'].isna()) & (df_merge_2015_Exp_250Country['Region']==Hep_grouped_by_region_status.index[i])]
        Hep_region_nan_df['Hepatitis B'] = Hep_grouped_by_region_status.values[i]
        df_merge_2015_Exp_250Country[(df_merge_2015_Exp_250Country['Hepatitis B'].isna()) & (df_merge_2015_Exp_250Country['Region']==Hep_grouped_by_region_status.index[i])]=Hep_region_nan_df

    return df_merge_2015_Exp_250Country

def Feature_eng(**context):

    df_merge_2015_Exp_250Country = context['task_instance'].xcom_pull(task_ids='Clean_data')

    df_merge_2015_Exp_250Country = df_merge_2015_Exp_250Country.rename(columns={"GDP":"GDP Per Capita"})

    df_merge_2015_Exp_250Country["GDP"] = df_merge_2015_Exp_250Country["GDP Per Capita"] * df_merge_2015_Exp_250Country["population"]
    pd.set_option('display.float_format', '{:.2f}'.format)

    df_merge_2015_Exp_250Country["Population/KM2"] = df_merge_2015_Exp_250Country["population"] / df_merge_2015_Exp_250Country["area"]

    HDI_SC_LE = df_merge_2015_Exp_250Country[["Country","Income composition of resources","Schooling","Life expectancy "]]


    HDI_SC_LE["LEI"] = (HDI_SC_LE["Life expectancy "] - 20)/(85-20)

    HDI_SC_LE["SI"] = (HDI_SC_LE["Schooling"] -0)/(18-0)


    HDI_SC_LE["II"] = (HDI_SC_LE["Income composition of resources"])**3/(HDI_SC_LE["SI"]*HDI_SC_LE["LEI"])


    HDI_SC_LE["Income Per Capita"] = (HDI_SC_LE["II"]*(75000-100))+100


    HDI_SC_LE.drop(["Income composition of resources","Schooling","Life expectancy "],axis=1,inplace=True)


    df_merge_2015_Exp_250Country = pd.merge(df_merge_2015_Exp_250Country,HDI_SC_LE, on='Country')

    df_merge_2015_Exp_250Country = df_merge_2015_Exp_250Country.rename(columns={"LEI":"Life Expectancy Index","SI":"Schooling Index","II":"Income Index","Income Per Capita":"Gross National Income Per Capita"})

    return df_merge_2015_Exp_250Country

t1 = PythonOperator(
    task_id='extract_data_250',
    provide_context=True,
    python_callable=extract_data_250_country,
    dag=dag,
)
t2 = PythonOperator(
    task_id='extract_data_LE',
    provide_context=True,
    python_callable=extract_data_Life_exp,
    dag=dag,
)
t3 = PythonOperator(
    task_id='extract_data_2015',
    provide_context=True,
    python_callable=extract_data_2015,
    dag=dag,
)
t4 = PythonOperator(
    task_id='extract_data_2019',
    provide_context=True,
    python_callable=extract_data_2019,
    dag=dag,
)
t5 = PythonOperator(
    task_id='extract_data_GDP',
    provide_context=True,
    python_callable=extract_data_GDP,
    dag=dag,
)
t6 = PythonOperator(
    task_id='merge_data',
    provide_context=True,
    python_callable=merge_data,
    dag=dag,
)
t7 = PythonOperator(
    task_id='Clean_data',
    provide_context=True,
    python_callable=data_cleaning,
    dag=dag,
)
t8 = PythonOperator(
    task_id='Feature_eng',
    provide_context=True,
    python_callable=Feature_eng,
    dag=dag,
)
t9 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
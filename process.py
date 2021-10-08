#Importing libraries
import pandas as pd
from google.cloud import bigquery
import json
import os 
import re


def obtainPath():
    with open('path.txt', 'r') as f:
        path = f.read()
    return path

def run():
    #Obtain path to BQ credentiasls.
    path = obtainPath()
    # Set enviroment variable GOOGLE_APPLICATION_CREDENTIALS
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=path
    #Let's initialize a bq client.
    #This dataset comes from a BQ-DB. Project's name is power-bi*
    BigQuery_client = bigquery.Client()
    query="""
    #standardSQL
    SELECT * FROM `power-bi-326113.analytics_264987177.events_*`
    """
    #Save results in query_results
    query_results = BigQuery_client.query(query)
    #Transform query_results to a pandas dataframe.
    df = pd.read_gbq(query)
    #So, that was the hard part. Now, we need to clean this dataset and get some important data such as city and country.
    #I'm going to use a list comprehension to do that, then I'll use some high level functions like map. I think both are very useful and very helpful to learn.
    df['city'] = [x['city'] for x in df['geo']]
    #df['country'] = [x['geo']['country'] for x in df['geo']]
    df['country'] = df['geo'].map(lambda x: x['country'])
    #We need page_location and I made this algorithm to obtain it.
    lista= []
    #print(df.event_params[0])
    for i in df.event_params:
            for j in i:
                    if j['key']=='page_location':
                            lista.append(j['value']['string_value'])
    lista2 = [re.search('hexaId=(.{5})', i).group(1) for i in lista if re.search('hexaId=(.{5})', i)!=None]
    lista3 = []
    for i in lista:
        try:
            if re.search('hexaId=(.{5})', i)!=None:
                lista3.append(re.search('hexaId=(.{5})', i).group(1))
            else:
                lista3.append('No tiene hexaId')
        except Exception as e:
            print('The operation was failed. ', e)
    df['hexaId'] = lista3
    df['mobile_brand_name'] =[i['mobile_brand_name'] for i in df.device ]
    df['device_category'] = [i['category'] for i in df.device ]

    df2 =df[['hexaId', 'event_date','event_timestamp' ,'event_name','country' ,'city', 'mobile_brand_name', 'device_category']]
    df3 = df2[df2['hexaId']!='No tiene hexaId']
    df3.hexaId.value_counts()












if '__name__'=='__main__':
    run()
# # Loading Libraries

# In[42]:

import datetime as dt
import json
import logging
import MySQLdb
import numpy as np
import os
import pandas as pd
import pymongo
import requests
import shutil
import sys
import time
import warnings
from collections import Counter
from datetime import date
from datetime import timedelta

from elasticsearch import Elasticsearch
from fbprophet import Prophet
from io import StringIO
from operator import add
from pymongo import MongoClient
from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine
from urlparse import urlparse


# # Function Definitions

# In[43]:

python_log_path = os.path.join(os.path.abspath(os.getcwd()), log_file_name)
warnings.filterwarnings("ignore")

###################################################################################
#############           DATA RESAMPLING                               #############
###################################################################################
    
def data_resampling(booking_data):
    booking_data = booking_data[["date", "y"]]
    booking_data = booking_data.rename(columns={'date': 'ds', 'y': 'y'})
    booking_data = booking_data.set_index("ds")
    booking_data = booking_data.resample("5T").agg(np.average)
    resampled_data = pd.DataFrame()
    resampled_data["y"] = pd.Series(booking_data["y"])
    resampled_data = resampled_data.reset_index()
    return resampled_data   
      
###################################################################################
#############                    LOGGER                               #############
###################################################################################  
def log_setup(python_log_path):
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # create a file handler
    logger.info('created directory {}'.format(python_log_path))
    handler = logging.FileHandler(python_log_path)
    handler.setLevel(logging.ERROR)
    # create a logging format
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [%(module)s,%(funcName)s,%(lineno)d] %(process)d ---: %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    return logger

###################################################################################
#############                       PREDICTOR                         #############
###################################################################################
def predict_results(model,freq):
    future = model.make_future_dataframe(
    periods=4 * 24 * no_of_days, freq="15T", include_history=False)
    results = model.predict(future)
    results1=results
    results = results[['ds', 'yhat']]
    results.loc[results.yhat < 0, "yhat"] = 0
    global time_of_creation
    time_of_creation=dt.datetime.now()
    results["creation_datetime"] = time_of_creation
    return results,results1  
    
###################################################################################
#############           SAVING TO SQL DATABASE                        #############
###################################################################################
def saving_results(comb_results):
    engine = create_engine(
        "mysql://{user}:{pw}@{add}/{db}".format(
            user=SqlCon['user'],
            pw=SqlCon['passwd'],
            add=SqlCon['host'],
            db=SqlCon['db']))
    comb_results.to_sql(con=engine, name=results_table, if_exists="replace")
    engine.dispose()
    return 1
    
###################################################################################
#############           CALCULATE BEST PRICE TIER                     #############
###################################################################################
def tier_calculation(results,tier_struct,max_count_vehicle_type):
    main_results=pd.DataFrame()
    for i in range(len(results)):
        data_segment=results[i:i+2]
        end_datetime = data_segment.iloc[-1]
        start_datetime = data_segment.iloc[0]
        sub_tier_struct=tier_struct[(tier_struct.place_id==place) & (tier_struct.vehicle_type==max_count_vehicle_type)]
        sub_tier_struct = sub_tier_struct[sub_tier_struct["tier_applicable_start_time"]
                                          <= start_datetime[0]]
        sub_tier_struct = sub_tier_struct[sub_tier_struct["tier_applicable_end_time"]
                                          >= end_datetime[0]]
        if sub_tier_struct.empty:
            logger.error('No pricing tier option avaiable to select for date time '+str(start_datetime[0]))
        n_tiers=sub_tier_struct["price_tier"].count()
        sub_tier_struct["ratio"]=sub_tier_struct["price"]/sub_tier_struct["tier_unit"]
        bins=np.linspace(0.0,1.0,n_tiers+1,True).astype(np.float)
        classes=np.digitize(data_segment.yhat,bins)
        sub_tier_struct=sub_tier_struct.sort_values("ratio")
        sub_tier_struct=sub_tier_struct.reset_index(drop=True)
        data_segment["bins"]=classes
        sub_tier_struct.insert(0,"bins",range(1,1+len(sub_tier_struct)))
        data_segment = pd.merge(data_segment, sub_tier_struct, on="bins")
        main_results=main_results.append(data_segment.iloc[0])
    return main_results 



###################################################################################
#############                    WRITING TO MQTT                      #############
###################################################################################
if not subset.empty:
        
        payload_data = subset[["placeId","pricingTier" ,"tierType"]]
        payload_data["timeStamp"]=milli_format
        print payload_data
        try:
            # converting the payload to json object
            payload_value = payload_data.to_json(orient="records")
        except Exception as e:
            logger.error('Exception occured during payload conversion'.format(e))
        client.connect(broker_address)
        try:
            client.publish(mqtt_topic, payload_value, qos=1,
                           retain=False)  # publishing the message
            print "published msg",payload_value
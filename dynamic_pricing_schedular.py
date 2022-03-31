
import datetime as dt
import json
import logging
import MySQLdb
import numpy as np
import os
import paho.mqtt.client as mqtt
import pandas as pd
import shutil
import threading
import time
import warnings

from dp_scheduler_config import *

# Define the logger function
python_log_path=os.path.join(os.path.abspath(os.getcwd()),log_file_name)
warnings.filterwarnings("ignore")
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

client = mqtt.Client(client_name) 

epoch = dt.datetime.utcfromtimestamp(0)


# In[ ]:

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000


def task_scheduler():
    # schedular will check every 60 secs (1min) to check the time
    threading.Timer(60, task_scheduler).start()
    try:
        con = MySQLdb.connect(
            host=sqlcon['host'], user=sqlcon['user'], passwd=sqlcon['passwd'], db=sqlcon['db'])
        cursor = con.cursor()
        # fetching data of pricing sturcture
        cursor.execute('SELECT * FROM ' +
                       sqlcon['db'] + '.' + sqlcon['table'] + ';')
        pred_data = cursor.fetchall()
        num_fields=len(cursor.description)
        field_names=[i[0] for i in cursor.description]
        cursor.close()
        con.close()
        pred_data = pd.DataFrame(np.array(pred_data), columns=field_names)
        pred_data = pred_data.astype(dtype={"placeId": "int64","pricingTier":"int64"})
    except Exception as e:
        logger.error('Exception occured during data fetching'.format(e),exc_info=True)
    live_datetime=dt.datetime.now()
    datetime_format=live_datetime.strftime("%Y-%m-%d %H:%M")
    milli_format=int(unix_time_millis(live_datetime))
    try:
        # converting string date to timestamp
        pred_data["timeStamp"] = pd.to_datetime(
            pred_data["timeStamp"], format="%Y-%m-%d %H:%M")
        pred_data["timeStamp"] = pred_data["ds"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M"))
    except Exception as e:
        pass  # if the data is already passed in timestamp
    print datetime_format
    subset = pred_data.loc[pred_data["timeStamp"] == datetime_format]
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
        except:
            logger.error(
                'Exception occured during message publishing. failed to publish at date-time :'+str(cur_date).format(e))
        client.disconnect()

task_scheduler()

"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import requests, json
import pandas as pd
import pendulum

# Setting timezone
local_tz = pendulum.timezone("Asia/Taipei")

# Default Arguments 
# 設定DAG及task執行細節
default_args = {

    "owner": "Austin", # Dag 所有人
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 2, 13, 10,tzinfo=local_tz), # 開始執行時間
    # "email": ["airflow@airflow.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": 1, # 失敗重新執行次數
    "retry_delay": timedelta(minutes=10), # 失敗重新執行時間
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def get_data():
    api_requests = requests.get('https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization=your_api_key&format=JSON&elementName=TEMP&parameterName%EF%BC%8C=CITY')

    # str to dict
    json_data = json.loads(api_requests.text)

    # 取出所要的資料
    data_list = json_data['records']['location']

    # 整理成所要的格式
    weather_info_list = []
    for data in data_list:
        weather_info = {
            "locationName" : data['locationName'],
            "lat" : data['lat'],
            "lon" : data['lon'],
            "stationId" : data['stationId'],
            "time" : data['time']['obsTime'],
            "temp" : data['weatherElement'][0]['elementValue']
        }
        weather_info_list.append(weather_info)

    # 透過pandas 存成.csv
    data_df = pd.DataFrame(weather_info_list)
    data_df.to_csv('/usr/local/airflow/dags/airflow_data.csv', ',', encoding='utf-8', mode= 'a', index= False)


# Create DAG並定義所屬task
# "weather_api_data" -> DAG id(唯一)
# default_args= default_args, 
# schedule_interval=timedelta(hours= 1) 設定排程多久執行一次
with DAG("weather_api_data", default_args= default_args, schedule_interval=timedelta(hours= 1)) as dag:
    
    # 一個DAG可由多個task組成，並且可定義每個task之間的相依關係
    get_data_task = PythonOperator(
                task_id = 'get_data',
                python_callable = get_data
                )


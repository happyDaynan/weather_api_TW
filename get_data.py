import requests, json
import pandas as pd


api_requests = requests.get('https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization=yorkey&format=JSON&elementName=TEMP&parameterName%EF%BC%8C=CITY')

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
df = pd.DataFrame(weather_info_list)
df.to_csv('./data.csv', ',', encoding='utf-8', index= False)
    
    



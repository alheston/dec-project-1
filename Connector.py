import requests
import pandas as pd
from datetime import datetime

# Penn Station: lat=40.7500792, lng=-73.9913481
# Hoboken: lat=40.7433066, lng=-74.0323752
# Stamford: lat=41.0534302, lng=-73.5387341
# Hackensack: lat=40.8871438, lng=-74.0410865

# Make a request to get data. Searches based on an arrival time. Arrive at the location no earlier than the given time.
# Specify multiple departure locations and one arrival location in each search
current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
url = f"https://api.traveltimeapp.com/v4/time-filter?type=driving&arrival_time={current_timestamp}&search_lat=40.7500792&search_lng=-73.9913481&locations=40.7433066_-74.0323752,41.0534302_-73.5387341,40.8871438_-74.0410865&app_id=ed4953bf&api_key=65b15716b757bccf8668bf3c04550682"
response = requests.get(url, verify=False)
print(response.status_code)
response_data=response.json()


# Extract data from the JSON response which we want and convert to dataframe
data = []
for result in response_data['results']:
    for location in result['locations']:
        data.append({
            'search_id': result['search_id'],
            'location_id': location['id'],
            'travel_time': location['properties'][0]['travel_time']
        })

df = pd.DataFrame(data)
print(df)
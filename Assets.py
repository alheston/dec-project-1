import pandas as pd
from Connector import TravelTimeApiClient





def extract_travel_time(response_data: dict)->pd.DataFrame:
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
import pandas as pd
from sqlalchemy import Table, MetaData
# from project.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from project.connectors.postgresql import PostgreSqlClient



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

    return pd.DataFrame(data)
     
    # print(df)



def add_columns(df: pd.DataFrame) -> pd.DataFrame:
    current_timestamp = datetime.now()
    df['load_timestamp'] = current_timestamp
    df['load_id'] = df['location_id'] + '_' + df['travel_time'].astype(str) + '_' + df['load_timestamp'].astype(str)
    return df
    

def load(
df: pd.DataFrame,
postgresql_client: PostgreSqlClient,
table: Table,
metadata: MetaData,
load_method: str
) -> None:

    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
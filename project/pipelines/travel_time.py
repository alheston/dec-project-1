from dotenv import load_dotenv
import os
from project.connectors.travel_time_api import TravelTimeApiClient
from project.assets.travel_time import extract_travel_time
from project.assets.travel_time import add_columns
from project.assets.travel_time import load
from project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime




if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    travel_time_api_client = TravelTimeApiClient(api_key = API_KEY,app_id = APP_ID)
    data = travel_time_api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(data)
    df_with_timestamp = add_columns(df_travel_time)
    # print(df_with_timestamp)

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    metadata = MetaData()
    table = Table(
        "travel_time_raw",
        metadata,
        Column("search_id", String),
        Column("location_id", String),
        Column("travel_time", Integer),
        Column("load_timestamp", DateTime),
        Column("load_id", String, primary_key=True)
    )
    load(df=df_with_timestamp, postgresql_client=postgresql_client, table=table, metadata=metadata)
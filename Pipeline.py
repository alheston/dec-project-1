from dotenv import load_dotenv
import os
from Connector import TravelTimeApiClient
from Assets import extract_travel_time
# from sqlalchemy import Table, MetaData, Column, Integer, String, Float




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
    response_data = travel_time_api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(response_data=response_data)
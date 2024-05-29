from dotenv import load_dotenv
from project.connectors.travel_time_api import TravelTimeApiClient
from project.assets.travel_time import extract_travel_time
from project.assets.travel_time import add_columns
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_weather_client_get_city_by_name(setup):
    API_KEY = os.environ.get("API_KEY")
    APP_ID = os.environ.get("APP_ID")
    api_client = TravelTimeApiClient(api_key=API_KEY, app_id = APP_ID)
    data = api_client.get_data(type="driving")
    df_travel_time = extract_travel_time(data)
    df_with_timestamp = add_columns(df_travel_time)

    assert type(df_with_timestamp) == dict
    assert len(df_with_timestamp) > 0

    python -m project_tests.connectors.test_travel_time_api
from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine


def extract(sql: str, engine: Engine) -> list[dict]:
    return [dict(row) for row in engine.execute(sql).all()]

def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata

def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata


def load(data: list[dict], table_name: str, engine: Engine, source_metadata: MetaData):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )

    table = target_metadata.tables[table_name]
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns},
    )
    engine.execute(upsert_statement)





if __name__ == "__main__":
    load_dotenv()
    SOURCE_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SOURCE_PORT = os.environ.get("PORT")

    source_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        host=SOURCE_SERVER_NAME,
        port=SOURCE_PORT,
        database=SOURCE_DATABASE_NAME,
    )
    source_engine = create_engine(source_connection_url)

    TARGET_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("DB_PASSWORD")
    TARGET_PORT = os.environ.get("PORT")

    target_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=TARGET_DB_USERNAME,
        password=TARGET_DB_PASSWORD,
        host=TARGET_SERVER_NAME,
        port=TARGET_PORT,
        database=TARGET_DATABASE_NAME,
    )
    target_engine = create_engine(target_connection_url)

    list_of_extraction_queries = [
        {
            "table": "travel_time_raw",
            "sql": f"select search_id, location_id, travel_time, load_timestamp, load_id from travel_time_raw",
        }
    ]

    data = extract(sql=list_of_extraction_queries[0].get("sql"), engine=source_engine)
    source_metadata = get_schema_metadata(engine=source_engine)
    load(
        data=data,
        table_name=list_of_extraction_queries[0].get("table"),
        engine=target_engine,
        source_metadata=source_metadata,
    )

from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template

def extract(sql: str, engine: Engine) -> list[dict]:
    return [dict(row) for row in engine.execute(sql).all()]

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

def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata

def load(data: list[dict], table_name: str, engine: Engine, source_metadata: MetaData):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    insert_statement = postgresql.insert(table).values(data)
    engine.execute(insert_statement)

def transform(engine: Engine, sql_template: Template, table_name: str):
    execute_sql = f"""
    drop table if exists {table_name};
    create table {table_name} as (
    {sql_template.render()}
    )
    """
    print(execute_sql)
    engine.execute(execute_sql)

if __name__ == "__main__":
    load_dotenv()

    SOURCE_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SOURCE_PORT = os.environ.get("PORT")

    source_connection = URL.create(
        drivername = "postgresql+pg8000",
        username = SOURCE_DB_USERNAME,
        password = SOURCE_DB_PASSWORD,
        host = SOURCE_SERVER_NAME,
        port = SOURCE_PORT,
        database = SOURCE_DATABASE_NAME
    )
    source_engine = create_engine(source_connection)

    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")

    target_connection = URL.create(
        drivername = "postgresql+pg8000",
        username = TARGET_DB_USERNAME,
        password = TARGET_DB_PASSWORD,
        host = TARGET_SERVER_NAME,
        port = TARGET_PORT,
        database = TARGET_DATABASE_NAME
    )
    target_engine = create_engine(target_connection)

    sql_env = Environment(loader=FileSystemLoader("sql/extract"))
    print(sql_env)

    for sql_path in sql_env.list_templates():
        sql_template = sql_env.get_template(sql_path)
        print(sql_template)
        table_name = sql_template.make_module().config.get("source_table_name")
        sql = sql_template.render()
        print(sql)
        data = extract(sql=sql, engine=source_engine)
        source_metadata = get_schema_metadata(engine=source_engine)
        load(
            data=data,
            table_name=table_name,
            engine=target_engine,
            source_metadata=source_metadata
        )

    transform_env = Environment(loader=FileSystemLoader("sql/transform"))
    transform_table_name = "travel_time_transform"
    transform_sql_template = transform_env.get_template(
        f"{transform_table_name}.sql"
    )

    transform(
        sql_template = transform_sql_template,
        engine = target_engine,
        table_name = transform_table_name
    )




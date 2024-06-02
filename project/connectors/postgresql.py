from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    Float,
)  
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:


    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5431,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name
        )
        print(f"Connection URL: {connection_url}")
        self.engine = create_engine(connection_url)




    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)
    
    def select_all(self, table: Table) -> list[dict]:
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def table_exists(self, table_name: str) -> bool:
        """
        Checks if the table already exists in the database.
        """
        return inspect(self.engine).has_table(table_name)

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)
    
    def run_sql(self, sql: str) -> list[dict]:
        """
        Execute SQL code provided and returns the result in a list of dictionaries.
        This method should only be used if you expect a resultset to be returned.
        """
        return [dict(row) for row in self.engine.execute(sql).all()]

    def drop_table(self, table_name: str) -> None:
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)

    def get_metadata(self) -> MetaData:
        """
        Gets the metadata object for all tables for a given database
        """
        metadata = MetaData(bind=self.engine)
        metadata.reflect()
        return metadata

    def get_table_schema(self, table_name: str) -> Table:
        """
        Gets the table schema and metadata
        """
        metadata = self.get_metadata()
        return metadata.tables[table_name], metadata


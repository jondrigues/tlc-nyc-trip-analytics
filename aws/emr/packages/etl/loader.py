from __future__ import annotations

import pandas
import pyspark
import os

from packages.utils.database_manager import DatabaseManager
from packages.utils.logger import Logger
from urllib.parse import urlparse, parse_qs


class Loader:

    def __init__(self, entity_name, filesystem_path) -> None:
        self.entity_name = entity_name
        self.filesystem_path = filesystem_path
        self._logger = Logger()
        super().__init__()

    def write_to_filesystem(self, layer: str, df: pyspark.sql.DataFrame, partitions: list | None = None,
                            mode: str = 'overwrite', partition_overwrite_mode: str = 'static') -> None:
        if partitions is None:
            partitions = []
        path = os.path.join(self.filesystem_path, layer, self.entity_name)
        self._logger.info(f"Writing file to {path}")
        df.write.partitionBy(*partitions)\
                .mode(mode)\
                .option('spark.sql.sources.partitionOverwriteMode', partition_overwrite_mode)\
                .parquet(path)
        self._logger.info(f"File successfully saved.")

    def write_to_database_with_pandas(self, conn_string: str, df: pandas.DataFrame, schema_name: str = 'public') -> None:
        self._logger.info("Connecting to Database")
        db_manager = DatabaseManager(conn_string=conn_string)
        db_manager.create_schema(schema_name=schema_name)
        self._logger.info(f"Writing {self.entity_name} to schema {self.schema_name}")
        db_manager.create_table_with_pandas_df(
            df=df,
            table_name=self.entity_name,
            schema_name=schema_name
        )

    def write_to_database_with_spark(self, conn_string: str, df: pandas.DataFrame, schema_name: str = 'public') -> None:
        self._logger.info("Connecting to Database")
        # db_manager = DatabaseManager(conn_string=conn_string)
        # db_manager.create_schema(schema_name=schema_name)
        self._logger.info(f"Writing {self.entity_name} to schema {schema_name}")
        scheme, host, port, database, username, password, params = self.parse_database_url(conn_string)

        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
            .option("dbtable", self.entity_name) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

    def parse_database_url(self, url):
        parsed_url = urlparse(url)

        scheme = parsed_url.scheme
        host = parsed_url.hostname
        port = parsed_url.port
        database = parsed_url.path.lstrip('/')
        username = parsed_url.username
        password = parsed_url.password

        # Obter parâmetros adicionais da query string
        query_params = parse_qs(parsed_url.query)
        params = {key: value[0] for key, value in query_params.items()}

        return scheme, host, port, database, username, password, params

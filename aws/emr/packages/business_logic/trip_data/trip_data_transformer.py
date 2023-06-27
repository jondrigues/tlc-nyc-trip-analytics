from __future__ import annotations

import pyspark

from packages.etl.transformer import Transformer
from pyspark.sql import SparkSession


class TripDataTransformer(Transformer):

    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, entity_name='trip_data')

    def apply_transformation(self, df: pyspark.sql.DataFrame | None) -> None:

        df = self._spark_session.sql("SELECT * FROM trusted_green UNION ALL SELECT * FROM trusted_yellow")

        df_aux = self._spark_session.read.csv(f"s3://{self.bucket_name}/aux_files/lat_lon.csv", sep=';', header=True)

        df_first_join = df.join(df_aux, [df_aux.location_id == df.PULocationID], 'LEFT')
        df_first_join = df_first_join.withColumnRenamed('Longitude', 'pu_longitude') \
                                     .withColumnRenamed('Latitude', 'pu_latitude')
        df_first_join = df_first_join.drop(*['borough', 'zone', 'location_id'])

        df_second_join = df_first_join.join(df_aux, [df_aux.location_id == df_first_join.DOLocationID], 'LEFT')
        df_final = df_second_join.withColumnRenamed('Longitude', 'do_longitude') \
                                 .withColumnRenamed('Latitude', 'do_latitude')
        df_final = df_final.drop(*['location_id'])

        self._loader.entity_name = 'trip_data'
        self._loader.write_to_filesystem(layer='refined', df=df_final, partitions=None)

    def execute(self) -> None:
        df_green = self._spark_session.read.parquet(f"{self.filesystem_path}/trusted/green")
        df_green.registerTempTable("trusted_green")
        df_yellow = self._spark_session.read.parquet(f"{self.filesystem_path}/trusted/yellow")
        df_yellow.registerTempTable("trusted_yellow")
        self.apply_transformation(None)





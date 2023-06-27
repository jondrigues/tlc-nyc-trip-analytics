import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from packages.etl.transformer import Transformer


class GreenTransformer(Transformer):

    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, entity_name='green')

    def apply_transformation(self, df: pyspark.sql.DataFrame):

        # Renomear colunas do DataFrame green
        df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
               .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

        df = df.withColumn('airport_fee', lit(0))

        df = df.withColumn('source', lit('green'))
        df = df.where("(trip_distance <= 30 and trip_distance > 0) "
                      "AND (passenger_count > 0 AND passenger_count < 5) "
                      "AND (pickup_datetime < dropoff_datetime)"
                      "AND (pickup_datetime >= '2018-01-01' and pickup_datetime < '2023-01-01')"
                      "AND (dropoff_datetime >= '2018-01-01' and dropoff_datetime < '2023-01-01')")
        return df

    def execute(self):
        df = self._spark_session.read.parquet(f"{self.filesystem_path}/raw/green")
        df_transformed = self.apply_transformation(df=df)
        self._loader.write_to_filesystem(layer='trusted', df=df_transformed)


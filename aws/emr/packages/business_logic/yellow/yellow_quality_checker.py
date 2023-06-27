from pyspark.sql import SparkSession

from packages.etl.soda_quality_checker import SodaQualityChecker


class YellowQualityChecker(SodaQualityChecker):
    checks = """
checks for yellow_df:
  - row_count > 0
  - max(trip_distance) <= 30
  - min(trip_distance) > 0
  - min(passenger_count) > 0
  - max(passenger_count) < 5
  - no_datetime_less_than_2018 = 0:
      no_datetime_less_than_2018 query: SELECT COUNT(*) FROM yellow_df WHERE pickup_datetime < '01-01-2018' or dropoff_datetime < '01-01-2018'
  - pickup_lower_than_dropoff = 0:
      pickup_lower_than_dropoff query: SELECT COUNT(*) FROM yellow_df WHERE pickup_datetime > dropoff_datetime
  - duplicate_lines_lower_than = 0:
      duplicate_lines_lower_than query: SELECT COUNT(foo.*) FROM (SELECT VendorID ,pickup_datetime ,dropoff_datetime ,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag ,PULocationID ,DOLocationID ,payment_type ,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,airport_fee, COUNT(*) FROM yellow_df GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 HAVING COUNT(*) > 1) as foo
  - schema:
      name: Confirm that required columns are present
      fail:
        when required column missing: [VendorID ,pickup_datetime ,dropoff_datetime ,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag ,PULocationID ,DOLocationID ,payment_type ,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,airport_fee]
        when wrong column type:
          VendorID: bigint
          pickup_datetime: timestamp
          dropoff_datetime: timestamp
          passenger_count: bigint
          trip_distance: double
          RatecodeID: bigint
          store_and_fwd_flag: string
          PULocationID: bigint
          DOLocationID: bigint
          payment_type: bigint
          fare_amount: double
          extra: double
          mta_tax: double
          tip_amount: double
          tolls_amount: double
          improvement_surcharge: double
          total_amount: double
          congestion_surcharge: double
          airport_fee: double   
"""

    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, entity_name='yellow')
from pyspark.sql import SparkSession

from packages.etl.soda_quality_checker import SodaQualityChecker


class GreenQualityChecker(SodaQualityChecker):
    checks = """
checks for green_df:
  - row_count > 0
  - min(passenger_count) > 0
  - max(passenger_count) < 5
  - no_datetime_less_than_2018 = 0:
      no_datetime_less_than_2018 query: SELECT COUNT(*) FROM green_df WHERE pickup_datetime < '01-01-2018' or dropoff_datetime < '01-01-2018'
  - pickup_lower_than_dropoff = 0:
      pickup_lower_than_dropoff query: SELECT COUNT(*) FROM green_df WHERE pickup_datetime > dropoff_datetime 
  - duplicate_lines_lower_than = 0:
      duplicate_lines_lower_than query: SELECT COUNT(foo.*) FROM (SELECT VendorID, pickup_datetime, dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge, COUNT(*) FROM green_df GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 HAVING COUNT(*) > 1) as foo
  - schema:
      name: Confirm that required columns are present
      fail:
        when required column missing: [VendorID, pickup_datetime, dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge]
        when wrong column type:
            VendorID: bigint
            pickup_datetime: timestamp
            dropoff_datetime: timestamp
            store_and_fwd_flag: string
            RatecodeID: bigint
            PULocationID: bigint
            DOLocationID: bigint
            passenger_count: bigint
            trip_distance: double
            fare_amount: double
            extra: double
            mta_tax: double
            tip_amount: double
            tolls_amount: double
            ehail_fee: int
            improvement_surcharge: double
            total_amount: double
            payment_type: double
            trip_type: double
            congestion_surcharge: double
"""

    def __init__(self, spark: SparkSession):
        super().__init__(spark=spark, entity_name='green')

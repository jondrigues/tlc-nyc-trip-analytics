from __future__ import annotations

from abc import abstractmethod

from pyspark.sql import SparkSession

import concurrent.futures
import multiprocessing

import time
from packages.etl.etl_base import ETLBase
from packages.utils.rest_api_hook import RestApiHook
from packages.utils.s3_helper import S3Helper
from io import BytesIO
import shutil
from pyspark.sql.functions import col, lit
from py4j.java_gateway import Py4JJavaError
import random


class RestApiExtractor(ETLBase):

    @property
    @abstractmethod
    def endpoint(self):
        raise NotImplementedError(
            "It is required to set 'endpoint' as class-level attribute."
        )

    @property
    @abstractmethod
    def schema(self):
        raise NotImplementedError(
            "It is required to set 'schema' as class-level attribute."
        )

    period = {
        "2018": [str(x).zfill(2) for x in range(2, 13)],
        "2019": [str(x).zfill(2) for x in range(1, 13)],
        "2020": [str(x).zfill(2) for x in range(1, 13)],
        "2021": [str(x).zfill(2) for x in range(1, 13)],
        "2022": [str(x).zfill(2) for x in range(1, 13)],
    }

    def __init__(self, spark: SparkSession, entity_name: str | None = None):
        self._api_hook = RestApiHook()
        self.s3_helper = S3Helper()
        super().__init__(spark=spark, entity_name=entity_name)

    def get_and_normalize_data(self, year, month, mode='append'):
        self.get_data(year, month)
        landing_df = self._spark_session.read.parquet(f"{self.filesystem_path}/landing/{self.get_file_name(year, month)}")
        self.normalize_data(landing_df, year, month, mode=mode)

    def get_file_name(self, year, month):
        return f'{self.entity_name}/{year}/{month}/{self.entity_name}_{year}-{month}.parquet'

    def get_data(self, year, month):
        url = self.endpoint.format(
            year=year,
            month=month,
        )

        time.sleep(random.randint(1, 5))
        result = self._api_hook.get(
            endpoint=url, headers=None, output_type="raw", retries=3, stream=True
        )

        data = BytesIO()
        shutil.copyfileobj(result, data)
        self.s3_helper.write_data_to_s3(
            data=data.getvalue(),
            bucket_name=self.bucket_name,
            s3_key=f'data_lake/landing/{self.get_file_name(year, month)}')

    def normalize_data(self, landing_df, year, month, mode='append'):
        try:
            schema_correct = all([field.name in landing_df.columns
                                  and field.dataType == landing_df.schema[field.name].dataType
                                  for field in self.schema.fields])
            landing_df = landing_df.withColumn('year', lit(year))
            landing_df = landing_df.withColumn('month', lit(month))
            if not schema_correct:
                self._logger.warning("Falling back to inferring schema.")

                for field in self.schema.fields:
                    column_name = field.name
                    data_type = field.dataType
                    landing_df = landing_df.withColumn(column_name, col(column_name).cast(data_type))

                self._loader.write_to_filesystem(layer='raw', df=landing_df, partitions=['year','month'],
                                                 partition_overwrite_mode='dynamic', mode=mode)
            else:
                self._loader.write_to_filesystem(layer='raw', df=landing_df, partitions=['year','month'],
                                                 partition_overwrite_mode='dynamic', mode=mode)
        except Py4JJavaError:
            self.s3_helper.delete_file_from_s3(
                bucket_name=self.bucket_name,
                s3_key=f'data_lake/landing/{self.get_file_name(year, month)}')
            self.get_and_normalize_data(year, month)
        except Exception as e:
            self._logger.error(f"Error processing file: {self.get_file_name(year, month)} with {e}")

    def execute(self):
        # start_time = time.perf_counter()
        # processes = []
        # for year, months in self.period.items():
        #     for month in months:
        #         t = Process(target=self.get_and_normalize_data, args=(year, month))
        #         t.start()
        #         processes.append(t)
        # for process in processes:
        #     process.join()
        # finish_time = time.perf_counter()
        # self._logger.info(f"Report retrieved in {finish_time - start_time} seconds")

        self.get_and_normalize_data('2018', '01', mode='overwrite')
        max_threads = multiprocessing.cpu_count() * 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for year, months in self.period.items():
                for month in months:
                    future = executor.submit(self.get_and_normalize_data, year, month)
                    futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                result = future.result()

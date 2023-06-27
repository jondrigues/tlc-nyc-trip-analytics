from __future__ import annotations

from abc import abstractmethod

import yaml
from pyspark.sql import SparkSession
from soda.scan import Scan

from packages.etl.etl_base import ETLBase


class SodaQualityChecker(ETLBase):

    @property
    @abstractmethod
    def checks(self):
        raise NotImplementedError(
            "It is required to set 'checks' as class-level attribute."
        )

    def __init__(self, spark: SparkSession, entity_name: str | None = None):
        super().__init__(spark=spark, entity_name=entity_name)

    def execute(self):
        df = self._spark_session.read.parquet(f"{self.filesystem_path}/trusted/{self.entity_name}")
        df.createOrReplaceTempView(f"{self.entity_name}_df")
        
        scan = Scan()
        scan.set_scan_definition_name("Datasets validation")
        scan.set_data_source_name("spark_df")
        scan.add_spark_session(self._spark_session)
        scan.add_sodacl_yaml_str(self.checks)
        scan.execute()
        scan.assert_no_error_logs()

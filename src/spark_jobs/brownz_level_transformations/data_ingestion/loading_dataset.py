
from pyspark.sql.functions import *
from datetime import date
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)
from utils.logging_utils import get_logger

class DataLoader:
    def __init__(self, spark, env: str, config: dict):
        self.spark = spark
        self.env = env.lower()          # normalize once
        self.config = config
        self.logger = get_logger("extracting_datsets") # one logger for the class

    def _get_path(self, dataset: str) -> str:
        """Helper to get correct path based on env."""
        key = f"{self.env}_raw_{dataset}"
        if key not in self.config:
            raise KeyError(f"Config key not found: {key}")
        return self.config[key]

    def load_patient_data(self, schema):
        path = self._get_path("patients")
        self.logger.info(f"Reading patients data from {path}")
        df = self.spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(path)
        bronze_df = (
        df
        .withColumn("ingestion_date", lit(date.today()))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("source_system", lit("faker_healthcare"))
        .withColumn("batch_id", lit(f"patient_{date.today()}"))
        )
        self.logger.info(f"Loaded {df.count()} records from patients.csv")
        return bronze_df

    def load_encounter_data(self, schema):
        path = self._get_path("encounters")
        self.logger.info(f"Reading encounters data from {path}")
        df = self.spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(path)
        bronze_df = (
        df
        .withColumn("ingestion_date", lit(date.today()))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("source_system", lit("faker_healthcare"))
        .withColumn("batch_id", lit(f"encounter_{date.today()}"))
        )
        self.logger.info(f"Loaded {df.count()} records from encounters.csv")
        return bronze_df

    def load_treatment_data(self, schema):
        path = self._get_path("treatments")
        self.logger.info(f"Reading treatments data from {path}")
        df = self.spark.read.format("csv") \
            .option("header", True) \
            .schema(schema) \
            .load(path)
        self.logger.info(f"Loaded {df.count()} records from treatments.csv")
        bronze_df = (
        df
        .withColumn("ingestion_date", lit(date.today()))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("source_system", lit("faker_healthcare"))
        .withColumn("batch_id", lit(f"treatment_{date.today()}"))
        )
        return bronze_df
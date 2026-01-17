
from pyspark.sql.functions import *
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import StringType, DateType, DoubleType

from utils.logging_utils import get_logger

# Allowed statuses
ENCOUNTER_STATUS = ['IN_PROGRESS', 'COMPLETED', 'CANCELLED']
TREATMENT_STATUS = ['SCHEDULED', 'COMPLETED', 'CANCELLED']


class DataValidator:
    """
    A class to validate datasets in a Spark environment.
    """

    def __init__(self, spark):
        self.spark = spark
        self.logger = get_logger("validating")

    def validate_patients(self, patients_df):

        self.logger.info("Validating patients dataset...")
        patients_df = patients_df.withColumn("patient_id", col("patient_id").cast("int")) \
                         .withColumn("date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd")) \
                         .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
        

        self.logger.info("Checking for nulls and duplicates in patients dataset...")
        if patients_df.filter(col("patient_id").isNull()).count() > 0:
            raise ValueError("Null patient_id found!")
        

        self.logger.info("Performing uniqueness check on patient_id...")
        if patients_df.select("patient_id").distinct().count() != patients_df.count():
            raise ValueError("Duplicate patient_id found!")
        
        return patients_df
    
    def validate_encounters(self, encounters_df):

        self.logger.info("Validating encounters dataset...")
        encounters_df = encounters_df.withColumn("patient_id", col("patient_id").cast("int")) \
                             .withColumn("encounter_timestamp", to_timestamp(col("encounter_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        
        self.logger.info("Checking for nulls, duplicates, and invalid values in encounters dataset...")
        if encounters_df.filter(col("encounter_id").isNull()).count() > 0:
            raise ValueError("Null encounter_id found!")
        
        self.logger.info("Performing uniqueness check on encounter_id...")
        if encounters_df.select("encounter_id").distinct().count() != encounters_df.count():
            raise ValueError("Duplicate encounter_id found!")
        
        # Validate status
        self.logger.info("Performing status validations...")
        invalid_status_enc = encounters_df.filter(~col("status").isin(ENCOUNTER_STATUS))
        if invalid_status_enc.count() > 0:
            raise ValueError("Invalid encounter status found!")
        
        return encounters_df
    

    def validate_treatments(self, treatments_df):

        self.logger.info("Validating treatments dataset...")
        treatments_df = treatments_df.withColumn("treatment_id", col("treatment_id").cast(StringType())) \
                             .withColumn("encounter_id", col("encounter_id").cast(StringType())) \
                             .withColumn("procedure_date", col("procedure_date").cast(DateType())) \
                             .withColumn("cost", col("cost").cast(DoubleType()))
        
        self.logger.info("Checking for nulls, duplicates, and invalid values in treatments dataset...")
        # Null check
        if treatments_df.filter(col("treatment_id").isNull()).count() > 0:
            raise ValueError("Null treatment_id found!")
        
        
        self.logger.info("Performing uniqueness and value checks...")
        # Uniqueness check
        if treatments_df.select("treatment_id").distinct().count() != treatments_df.count():
            raise ValueError("Duplicate treatment_id found!")
        
        self.logger.info("Performing status and cost validations...")
        # Status validation
        if treatments_df.filter(~col("status").isin(TREATMENT_STATUS)).count() > 0:
            raise ValueError("Invalid treatment status found!")
        
        self.logger.info("All treatment statuses are valid.")
        # Cost validation
        if treatments_df.filter(col("cost") <= 0).count() > 0:
            raise ValueError("Invalid treatment cost found!")
        
        return treatments_df
    




                
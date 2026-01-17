from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from utils.logging_utils import get_logger
class SilverTransformer:
    """
    Silver layer transformations:
    - Clean data
    - Enforce Foreign Key relationships
    - Remove duplicates
    - Prepare datasets for analytics
    """

    def __init__(self, spark):
        self.spark = spark
        self.logger = get_logger("SilverLevelTransformer")

    # -----------------------------
    # PATIENTS (Dimension)
    # -----------------------------
    def transform_patients(self, patients_df: DataFrame) -> DataFrame:
        self.logger.info("Starting Silver transformation for patients")

        # Drop exact duplicates (safety)
        patients_df = patients_df.dropDuplicates(["patient_id"])

        # Handle missing non-critical fields
        patients_df = patients_df.fillna({
            "insurance_type": "UNKNOWN",
            "gender": "UNKNOWN"
        })

        self.logger.info("Patients Silver transformation completed")
        return patients_df

    # -----------------------------
    # ENCOUNTERS (Fact)
    # -----------------------------
    def transform_encounters(self, encounters_df: DataFrame, patients_df: DataFrame) -> DataFrame:

        self.logger.info("Starting Silver transformation for encounters")

        # Remove duplicates
        encounters_df = encounters_df.dropDuplicates(["encounter_id"])

        # FK validation: encounter.patient_id → patients.patient_id
        valid_encounters = encounters_df.join(
            patients_df.select("patient_id"),
            on="patient_id",
            how="inner"
        )

        invalid_encounters = encounters_df.join(
            patients_df.select("patient_id"),
            on="patient_id",
            how="left_anti"
        )

        if invalid_encounters.count() > 0:
            self.logger.error("Orphan encounters found (no matching patient)")
            invalid_encounters.show(truncate=False)
            raise ValueError("Foreign key violation: encounter.patient_id")

        self.logger.info("Encounters Foreign Key validation passed")

        return valid_encounters

    # -----------------------------
    # TREATMENTS (Fact)
    # -----------------------------
    def transform_treatments(
        self,
        treatments_df: DataFrame,
        encounters_df: DataFrame
    ) -> DataFrame:

        self.logger.info("Starting Silver transformation for treatments")

        # Remove duplicates
        treatments_df = treatments_df.dropDuplicates(["treatment_id"])

        # FK validation: treatment.encounter_id → encounters.encounter_id
        valid_treatments = treatments_df.join(
            encounters_df.select("encounter_id"),
            on="encounter_id",
            how="inner"
        )

        invalid_treatments = treatments_df.join(
            encounters_df.select("encounter_id"),
            on="encounter_id",
            how="left_anti"
        )

        if invalid_treatments.count() > 0:
            self.logger.error("Orphan treatments found (no matching encounter)")
            invalid_treatments.show(truncate=False)
            raise ValueError("Foreign key violation: treatment.encounter_id")

        # Business rule:
        # Only COMPLETED treatments are billable
        valid_treatments = valid_treatments.withColumn(
            "is_billable",
            col("status") == lit("COMPLETED")
        )

        self.logger.info("Treatments Silver transformation completed")
        return valid_treatments



















from pyspark.sql import DataFrame
from utils.logging_utils import get_logger
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    sum, count, when, lit
)



class GoldTransformer:
    """
    Gold layer:
    - Build dimensions
    - Build fact tables
    - Compute KPIs
    """

    def __init__(self, spark):
        self.spark = spark
        self.logger = self.logger = get_logger("GoldLevelTransformer")

    # =========================
    # DIMENSIONS
    # =========================

    def build_dim_patient(self, patients_df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_patient")

        return patients_df.select(
            "patient_id",
            "first_name",
            "last_name",
            "gender",
            "date_of_birth",
            "insurance_type",
            "registration_date"
        ).dropDuplicates(["patient_id"])

    def build_dim_doctor(self, encounters_df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_doctor")

        return encounters_df.select(
            "doctor_id"
        ).dropDuplicates()

    def build_dim_hospital_unit(self, encounters_df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_hospital_unit")

        return encounters_df.select(
            "hospital_unit"
        ).dropDuplicates()

    def build_dim_date(self, df: DataFrame, date_col: str) -> DataFrame:
        self.logger.info("Building dim_date")

        return df.select(col(date_col).alias("date")).dropDuplicates() \
            .withColumn("year", year("date")) \
            .withColumn("month", month("date")) \
            .withColumn("day", dayofmonth("date"))

    # =========================
    # FACT TABLES
    # =========================

    def build_fact_encounters(
        self,
        encounters_df: DataFrame,
        patients_df: DataFrame
    ) -> DataFrame:
        self.logger.info("Building fact_encounters")

        return encounters_df.join(
            patients_df,
            on="patient_id",
            how="inner"
        ).select(
            "encounter_id",
            "patient_id",
            "doctor_id",
            "hospital_unit",
            "encounter_type",
            "diagnosis",
            "encounter_timestamp",
            "status"
        )

    def build_fact_treatments(
        self,
        treatments_df: DataFrame,
        encounters_df: DataFrame
    ) -> DataFrame:
        self.logger.info("Building fact_treatments")

        return treatments_df.join(
            encounters_df.select(
                "encounter_id",
                "patient_id",
                "hospital_unit",
                "diagnosis"
            ),
            on="encounter_id",
            how="inner"
        ).select(
            "treatment_id",
            "encounter_id",
            "patient_id",
            "hospital_unit",
            "diagnosis",
            "procedure_name",
            "procedure_date",
            "cost",
            "status",
            "is_billable"
        )

    # =========================
    # KPI AGGREGATIONS
    # =========================

    def kpi_encounter_counts(self, fact_encounters: DataFrame) -> DataFrame:
        self.logger.info("Calculating encounter counts")

        return fact_encounters.groupBy(
            "hospital_unit",
            "encounter_type"
        ).agg(
            count("*").alias("total_encounters")
        )

    def kpi_treatment_costs(self, fact_treatments: DataFrame) -> DataFrame:
        self.logger.info("Calculating treatment costs")

        return fact_treatments.filter(col("status") == "COMPLETED") \
            .groupBy("procedure_name") \
            .agg(
                sum("cost").alias("total_cost")
            )

    def kpi_cancellation_rate(self, fact_treatments: DataFrame) -> DataFrame:
        self.logger.info("Calculating treatment cancellation rates")

        return fact_treatments.groupBy("procedure_name") \
            .agg(
                count("*").alias("total_treatments"),
                sum(
                    when(col("status") == "CANCELLED", 1).otherwise(0)
                ).alias("cancelled_count")
            ).withColumn(
                "cancellation_rate",
                col("cancelled_count") / col("total_treatments")
            )

    def kpi_revenue_loss(self, fact_treatments: DataFrame) -> DataFrame:
        self.logger.info("Calculating revenue loss from cancelled treatments")

        return fact_treatments.filter(col("status") == "CANCELLED") \
            .groupBy("procedure_name") \
            .agg(
                sum("cost").alias("revenue_lost")
            )






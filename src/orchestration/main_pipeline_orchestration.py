import json
import os

from utils.create_sparksession import creating_spark_session
from src.spark_jobs.brownz_level_transformations.data_ingestion.loading_dataset import DataLoader
from src.spark_jobs.brownz_level_transformations.validations.datasets_validation import DataValidator
from spark_jobs.silver_level_transformations.slivereLevel_clenaning_datasets import SilverTransformer
from spark_jobs.goldLevel_transformations.gold_analytics_transformations import GoldTransformer
from writer.writting_data import DataWriter
from schemas.spark_datasets_schemas import Manual_schema
from utils.logging_utils import get_logger





def run_healthcare_data_pipeline():

    config_path = os.getenv("PIPELINE_CONFIG_PATH")

    if not config_path:
        raise RuntimeError("PIPELINE_CONFIG_PATH environment variable is not set")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    cs = creating_spark_session()
    spark = cs.create_spark_session("s3")      # we call s3 or local or doc_airflow

    ms = Manual_schema
    Eschema = ms.encounter_schema()
    pschema = ms.patient_schema()
    Tschema = ms.treatment_schema()

    w = DataWriter(config, "s3")                # we call s3 or local or doc_airflow
    d = DataLoader(spark, "s3", config)        # we call s3 or local or doc_airflow

    pdf = d.load_patient_data(pschema)
    edf = d.load_encounter_data(Eschema)
    tdf = d.load_treatment_data(Tschema)

    v = DataValidator(spark)
    validated_pdf = v.validate_patients(pdf)
    validated_edf = v.validate_encounters(edf)
    validated_tdf = v.validate_treatments(tdf)

    w.writing_bronzeLevel_data(
        validated_pdf, validated_edf, validated_tdf, "bronze_level"
    )

    s = SilverTransformer(spark)
    silver_pdf = s.transform_patients(validated_pdf)
    silver_edf = s.transform_encounters(validated_edf, silver_pdf)
    silver_tdf = s.transform_treatments(validated_tdf, silver_edf)

    w.writing_silverLevel_data(
        silver_pdf, silver_edf, silver_tdf, "silver_level"
    )

    g = GoldTransformer(spark)
    dim_patient_df = g.build_dim_patient(silver_pdf)
    dim_doctor_df = g.build_dim_doctor(silver_edf)
    dim_hospital_unit_df = g.build_dim_hospital_unit(silver_edf)
    dim_date_df = g.build_dim_date(silver_edf, "encounter_timestamp")
    fact_encounters_df = g.build_fact_encounters(silver_edf, silver_pdf)
    fact_treatments_df = g.build_fact_treatments(silver_tdf, silver_edf)

    kpi_encounter_counts_df = g.kpi_encounter_counts(fact_encounters_df)
    kpi_treatment_costs_df = g.kpi_treatment_costs(fact_treatments_df)
    kpi_cancellation_rate_df = g.kpi_cancellation_rate(fact_treatments_df)
    kpi_revenue_loss_df = g.kpi_revenue_loss(fact_treatments_df)

    w.writing_goldLevel_data(
        dim_patient_df,
        dim_doctor_df,
        dim_hospital_unit_df,
        dim_date_df,
        fact_encounters_df,
        fact_treatments_df,
        "gold_level",
    )


if __name__ == "__main__":
    run_healthcare_data_pipeline()

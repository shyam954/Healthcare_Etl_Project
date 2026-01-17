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






class Run_pipline():
    def __init__(self):
        
        config_path = os.getenv("PIPELINE_CONFIG_PATH")

        if not config_path:
            raise RuntimeError("PIPELINE_CONFIG_PATH environment variable is not set")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")

        with open(config_path, "r") as f:
            self.config = json.load(f)

        cs = creating_spark_session()
        self.spark = cs.create_spark_session("doc_airflow")
        ms = Manual_schema
        self.Eschema = ms.encounter_schema()
        self.pschema = ms.patient_schema()
        self.Tschema = ms.treatment_schema()
        self.w = DataWriter(self.config, "doc_airflow")

    def runnging_data_extracting_step(self):

      
        d = DataLoader(self.spark, "doc_airflow", self.config)

        pdf = d.load_patient_data(self.pschema)
        edf = d.load_encounter_data(self.Eschema)
        tdf = d.load_treatment_data(self.Tschema)
        
        return{"pdf":pdf,"edf":edf,"tdf":tdf}


    def runnging_data_validation_step(self):
        v = DataValidator(self.spark)
        DE=self.runnging_data_extracting_step()
        validated_pdf = v.validate_patients(DE["pdf"])
        validated_edf = v.validate_encounters(DE["edf"])
        validated_tdf = v.validate_treatments(DE["tdf"])
        self.w.writing_bronzeLevel_data(
            validated_pdf, validated_edf, validated_tdf, "bronze_level"
        )
        return{"validated_pdf":validated_pdf,"validated_edf":validated_edf,"validated_tdf":validated_tdf}

      
    def runnging_silver_transformations_step(self):
        BV=self.runnging_data_validation_step()


        s = SilverTransformer(self.spark)
        silver_pdf = s.transform_patients(BV["validated_pdf"])
        silver_edf = s.transform_encounters(BV["validated_edf"], BV["validated_pdf"])
        silver_tdf = s.transform_treatments(BV["validated_tdf"], BV["validated_edf"])

        self.w.writing_silverLevel_data(
            silver_pdf, silver_edf, silver_tdf, "silver_level"
        )
        return{"silver_pdf":silver_pdf,"silver_edf":silver_edf,"silver_tdf":silver_tdf}
     
    def runnging_gold_transformations_step(self):
        ST=self.runnging_silver_transformations_step()
        g = GoldTransformer(self.spark)

        dim_patient_df = g.build_dim_patient(ST["silver_pdf"])
        dim_doctor_df = g.build_dim_doctor(ST["silver_edf"])
        dim_hospital_unit_df = g.build_dim_hospital_unit(ST["silver_edf"])
        dim_date_df = g.build_dim_date(ST["silver_edf"], "encounter_timestamp")
        fact_encounters_df = g.build_fact_encounters(ST["silver_edf"],ST["silver_pdf"])
        fact_treatments_df = g.build_fact_treatments(ST["silver_tdf"], ST["silver_edf"])

        kpi_encounter_counts_df = g.kpi_encounter_counts(fact_encounters_df)
        kpi_treatment_costs_df = g.kpi_treatment_costs(fact_treatments_df)
        kpi_cancellation_rate_df = g.kpi_cancellation_rate(fact_treatments_df)
        kpi_revenue_loss_df = g.kpi_revenue_loss(fact_treatments_df)

        self.w.writing_goldLevel_data(
            dim_patient_df,
            dim_doctor_df,
            dim_hospital_unit_df,
            dim_date_df,
            fact_encounters_df,
            fact_treatments_df,
            "gold_level",
        )




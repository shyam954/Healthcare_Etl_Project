from utils.logging_utils import get_logger

class DataWriter:
    def __init__(self,config,env: str = "local") -> None:
        self.logger = get_logger("writing_output")
        self.config = config
        self.local_base_path = config["local_processed_path"]
        self.s3_output_path=config["s3_output_path"]
        self.da_output_path=config["local_processed_path_doc_airflow"]
        self.env = env

    def writing_bronzeLevel_data(self ,patients_df, encounters_df, treatments_df,path: str, formatt: str = "parquet", modee: str = "overwrite"):
        """
        Write DataFrame to specified path in given format.

        :param df: DataFrame to write
        :param path: Destination path
        :param format: File format (default is 'parquet')
        :param mode: Write mode (default is 'overwrite')
        """
       
        if self.env.lower() =="local":
            self.logger.info("Writing Bronze level data...")
            print(self.local_base_path)

            patients_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.local_base_path}bronze_patients")

            encounters_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.local_base_path}bronze_encounters/")

            treatments_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.local_base_path}bronze_treatments/")

        elif self.env.lower() =="s3":
            patients_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.s3_output_path}bronze_patients/")

            encounters_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.s3_output_path}bronze_encounters/")

            treatments_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.s3_output_path}bronze_treatments/")

        elif self.env.lower() =="doc_airflow":
            patients_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}bronze_patients/")
            encounters_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}bronze_encounters/")
            treatments_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}bronze_treatments/")


        self.logger.info("brownz level data written.")
       

        
    def writing_silverLevel_data(self, patients_df, encounters_df, treatments_df, path: str, formatt: str = "parquet", modee: str = "overwrite") -> None:
        """
        Write DataFrame to specified path in given format.

        :param df: DataFrame to write
        :param path: Destination path
        :param format: File format (default is 'parquet')
        :param mode: Write mode (default is 'overwrite')
        """
        self.logger.info("Writing Silver level data...")
        if self.env.lower() =="local":
            patients_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.local_base_path}Silver_patients/")

            encounters_df.write \
            .mode("overwrite") \
            .parquet(f"{self.local_base_path}Silver_encounters/")

            treatments_df.write \
            .mode("overwrite") \
            .parquet(f"{self.local_base_path}Silver_treatments/")

        elif self.env.lower() =="s3":
            patients_df.write \
            .mode("overwrite") \
            .parquet(f"{self.s3_output_path}Silver_patients/")
            encounters_df.write \
            .mode("overwrite") \
            .parquet(f"{self.s3_output_path}Silver_encounters/")
            treatments_df.write \
            .mode("overwrite") \
            .parquet(f"{self.s3_output_path}Silver_treatments/")

        elif self.env.lower() =="doc_airflow":
            patients_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}Silver_patients/")
            encounters_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}Silver_encounters/")
            treatments_df.write \
            .mode(modee) \
            .format(formatt).save(f"{self.da_output_path}Silver_treatments/")

        
        self.logger.info("Silver level data written.")

    def writing_goldLevel_data(self, dim_patients_df, dim_doctor_df, dim_hospital_unit_df, dim_date_df, fact_encounters_df, fact_treatments_df, path: str, format: str = "parquet", mode: str = "overwrite") -> None:
        """
        Write DataFrame to specified path in given format.

        :param df: DataFrame to write
        :param path: Destination path
        :param format: File format (default is 'parquet')
        :param mode: Write mode (default is 'overwrite')
        """
        self.logger.info("Writing Gold level data...")
        if self.env.lower() =="local":
            raw_base_path = self.local_base_path
            dim_patients_df.write.mode("overwrite").parquet(f"{raw_base_path}golddim_patients")
            dim_doctor_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_doctor")
            dim_hospital_unit_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_hospital_unit")
            dim_date_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_date")
            fact_encounters_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_encounters")
            fact_treatments_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_treatments")

        elif self.env.lower() =="s3":
            raw_base_path = self.s3_output_path
            dim_patients_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_patients")
            dim_doctor_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_doctor")
            dim_hospital_unit_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_hospital_unit")
            dim_date_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_date")
            fact_encounters_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_encounters")
            fact_treatments_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_treatments")

        elif self.env.lower() =="doc_airflow":
            raw_base_path = self.da_output_path
            dim_patients_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_patients")
            dim_doctor_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_doctor")
            dim_hospital_unit_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_hospital_unit")
            dim_date_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_dim_date")
            fact_encounters_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_encounters")
            fact_treatments_df.write.mode("overwrite").parquet(f"{raw_base_path}gold_fact_treatments")
        self.logger.info("Gold level data written.")

        
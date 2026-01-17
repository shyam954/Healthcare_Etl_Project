import json
from pyspark.sql import SparkSession
class creating_spark_session():
    def create_spark_session(self,env,app_name="HealthCareETL"):

        env = env.lower()
        if env in  [ "local","doc_airflow"]:
            spark = SparkSession.builder.master("local[*]").appName("HealthCareETL").getOrCreate()
            return spark 
        elif env == "s3":
            return (
                SparkSession.builder
                .appName("HealthCareETL")
                # dynamic overwrite for partitions
                .config("spark.ui.port", "4040")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                # S3A filesystem
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                # AWS credentials from ~/.aws/credentials
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                # add Hadoop AWS and AWS SDK JARs
                .config("spark.jars.packages",
                        "org.apache.hadoop:hadoop-aws:3.3.4,"
                        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
                .getOrCreate()
            )
        



    

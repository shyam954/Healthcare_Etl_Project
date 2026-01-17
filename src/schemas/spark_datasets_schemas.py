from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,DoubleType
)





class Manual_schema:
    """
    This class contains manually defined Spark DataFrame schemas for various datasets.
    Each schema is defined as a StructType with appropriate field names and data types.
    """
    def patient_schema() :
            ps=StructType([
                    StructField("patient_id",       IntegerType(), nullable=False),   # unique ID, should not be null
                    StructField("first_name",       StringType(),  nullable=True),
                    StructField("last_name",        StringType(),  nullable=True),
                    StructField("gender",           StringType(),  nullable=True),   # M/F (though data has inconsistencies)
                    StructField("date_of_birth",    StringType(),    nullable=True),
                    StructField("email",            StringType(),  nullable=True),
                    StructField("phone_number",     StringType(),  nullable=True),   # phones are strings due to formats, extensions, etc.
                    StructField("insurance_type",   StringType(),  nullable=True),
                    StructField("registration_date", StringType(),   nullable=True)
                ])
            return ps
    def encounter_schema() :
          
        es=StructType([
        StructField("encounter_id",      StringType(),  nullable=False),   # e.g., ENC00001 – alphanumeric prefix + number
        StructField("patient_id",        IntegerType(), nullable=False),   # foreign key to patients table
        StructField("encounter_timestamp",StringType(), nullable=True),  # full datetime: yyyy-MM-dd HH:mm:ss
        StructField("encounter_type",    StringType(),  nullable=True),    # INPATIENT, EMERGENCY, OUTPATIENT, etc.
        StructField("diagnosis",         StringType(),  nullable=True),    # free-text like COVID-19, Fracture, etc.
        StructField("doctor_id",         StringType(),  nullable=True),    # e.g., DR157 – alphanumeric
        StructField("hospital_unit",     StringType(),  nullable=True),    # NEUROLOGY, CARDIOLOGY, GENERAL, ORTHOPEDICS, etc.
        StructField("status",            StringType(),  nullable=True)     # IN_PROGRESS, CANCELLED, COMPLETED
        ])
        return es
    

    def treatment_schema    () :
        ts=StructType([
        StructField("treatment_id",   StringType(),  nullable=False),   # e.g., TRT0001 – alphanumeric prefix + number
        StructField("encounter_id",   StringType(),  nullable=False),   # foreign key to encounters, e.g., ENC03895
        StructField("procedure_name", StringType(),  nullable=True),    # SURGERY, CT SCAN, MRI, BLOOD TEST, X-RAY, etc.
        StructField("procedure_date", DateType(),    nullable=True),    # yyyy-MM-dd format
        StructField("cost",           DoubleType(),  nullable=True),    # monetary value with decimals
        StructField("status",         StringType(),  nullable=True)     # SCHEDULED, CANCELLED, COMPLETED
        ])
        return ts
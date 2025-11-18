from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, sum as _sum, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import great_expectations as gx
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator


spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()


schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

input_csv_path = "data/202306-divvy-tripdata.csv"
df = spark.read.csv(input_csv_path, header=True, schema=schema, mode="DROPMALFORMED")


df = df.withColumn("started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("duration_seconds", unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) \
       .withColumn("date", date_format(col("started_at"), "yyyy-MM-dd"))

daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

# same_day = 1 → valid (≤ 24 hr), 0 → abnormal (> 24 hr)
daily_durations = daily_durations.withColumn(
    "same_day",
    when(col("total_duration_seconds") / 3600 > 24, 0).otherwise(1)
)

# -------------------------------------
# Great Expectations setup
# -------------------------------------
context = gx.get_context(mode="ephemeral")               
execution_engine = SparkDFExecutionEngine(spark)  
batch = Batch(data=daily_durations)
validator = Validator(execution_engine=execution_engine, batches=[batch])


# Fail if any trip is not same-day (i.e., same_day == 0)
validator.expect_column_values_to_not_be_in_set("same_day", [0])


results = validator.validate()


def show_data_quality_alert(results):
    failed = [r for r in results["results"] if not r["success"]]
    
    print("\n DATA QUALITY VALIDATION SUMMARY ")
    print(json.dumps(results["statistics"], indent=2))
    
    if failed:
        print("\n ALERT: Data Quality Checks Failed!")
        for f in failed:
            
            if isinstance(f["expectation_config"], dict):
                
                exp = f["expectation_config"].get("expectation_type", "Unknown expectation")
            else:
               
                exp = getattr(f["expectation_config"], "expectation_type", "Unknown expectation")
                
           
            if isinstance(f["expectation_config"], dict) and "kwargs" in f["expectation_config"]:
                col = f["expectation_config"]["kwargs"].get("column", "Unknown column")
            else:
                
                kwargs = getattr(f["expectation_config"], "kwargs", {})
                col = kwargs.get("column", "Unknown column") if isinstance(kwargs, dict) else "Unknown column"
                
            print(f" - Expectation `{exp}` failed for column: {col}")
        print("\n Recommendation: Inspect total_duration_seconds column for abnormal values.\n")
    else:
        print("\n All data quality checks passed successfully.\n")

show_data_quality_alert(results)


output_parquet_path = "results/output_file.parquet"
daily_durations.coalesce(1).write.mode("overwrite").parquet(output_parquet_path)
daily_durations.coalesce(1).write.csv("results/output_csv", header=True, mode="overwrite")
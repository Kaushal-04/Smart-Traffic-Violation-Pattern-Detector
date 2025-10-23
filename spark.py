from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp, col, trim, lower, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TrafficViolationData_Cleaning") \
    .master("local[*]") \
    .getOrCreate()

# Define schema
traffic_schema = StructType([
    StructField("Violation_ID", StringType(), False),
    StructField("Timestamp", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Violation_Type", StringType(), True),
    StructField("Vehicle_Type", StringType(), True),
    StructField("Severity", IntegerType(), True)
])

# Load raw JSON data
json_path = "traffic_violations.json"
raw_df = spark.read.option("multiline", True) \
    .option("mode", "PERMISSIVE") \
    .schema(traffic_schema) \
    .json(json_path)

print("\n=== Raw JSON Data ===")
raw_df.show(truncate=False)

# -----------------------------------------------------------------------------------------------------------------

# Handle missing or null values
clean_df = raw_df.na.drop(subset=["Violation_ID", "Timestamp", "Violation_Type"]) \
    .fill({"Location": "Unknown", "Vehicle_Type": "Unknown", "Severity": 0})

# Standardize timestamps
clean_df = clean_df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Standardize categorical fields
clean_df = clean_df.withColumn("Violation_Type", trim(lower(col("Violation_Type")))) \
    .withColumn("Vehicle_Type", trim(lower(col("Vehicle_Type")))) \
    .withColumn("Location", trim(col("Location")))

# Validate violation types against expected list
expected_violations = ["speeding", "signal violation", "parking", "drunk driving", "seat belt", "no helmet"]

clean_df = clean_df.withColumn(
    "Violation_Type",
    when(col("Violation_Type").isin(expected_violations), col("Violation_Type")).otherwise("other")
)


print("\n=== Cleaned Data ===")
clean_df.show(truncate=False)

# Save cleaned data into Parquet format for fast querying
parquet_path = "cleaned_traffic_violations.parquet"
clean_df.write.mode("overwrite").parquet(parquet_path)


# Stop Spark session
spark.stop()

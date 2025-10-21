from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TrafficViolationData_JSON") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for traffic violations
traffic_schema = StructType([
    StructField("Violation_ID", StringType(), False),
    StructField("Timestamp", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Violation_Type", StringType(), True),
    StructField("Vehicle_Type", StringType(), True),
    StructField("Severity", IntegerType(), True)
])

# Load JSON data
json_path = "traffic_violations.json"
json_df = spark.read.option("multiline", True) \
    .schema(traffic_schema) \
    .json(json_path)

print("\n=== Raw JSON Data ===")
json_df.show(truncate=False)

# Stop Spark session
spark.stop()

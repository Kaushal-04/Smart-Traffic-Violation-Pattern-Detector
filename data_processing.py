import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, coalesce, try_to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# This points Spark to the winutils.exe and hadoop.dll
if sys.platform == "win32" and 'HADOOP_HOME' not in os.environ:
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'

def process_traffic_data(input_path, output_path):   
    spark = SparkSession.builder \
        .appName("TrafficViolationDataProcessing") \
        .master("local[*]") \
        .getOrCreate()
        
    print(f"Spark Session initialized. Processing data from {input_path}...")

    schema = StructType([
        StructField("Violation_ID", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Violation_Type", StringType(), True),
        StructField("Vehicle_Type", StringType(), True),
        StructField("Severity", StringType(), True)
    ])

    try:
        df = spark.read.schema(schema).json(input_path)
    except Exception as e:
        print(f"ERROR: Could not read JSON file from {input_path}.", file=sys.stderr)
        print(f"Exception details: {e}", file=sys.stderr)
        spark.stop()
        return

    print("\n=== Raw JSON Data Schema and Sample ===")
    df.printSchema()
    df.show(5, truncate=False)

    try:
        # Step 1: Trim whitespace and convert empty strings to NULL
        df = df.select([
            when(trim(col(c)) == "", None).otherwise(trim(col(c))).alias(c)
            for c in df.columns
        ])

        # Step 2: Convert Timestamp safely
        df = df.withColumn(
            "Timestamp",
            coalesce(
                try_to_timestamp(col("Timestamp"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("Timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            )
        )

        # Step 3: Cast Severity to Integer
        df = df.withColumn("Severity", col("Severity").cast(IntegerType()))

        # Step 4: Fill missing Location values
        df = df.withColumn("Location", 
                           when(col("Location").isNull(), lit("Unknown"))
                           .otherwise(col("Location"))
        )

        # Step 5: Drop rows with missing essential fields
        initial_count = df.count()
        clean_df = df.dropna(subset=["Violation_ID", "Violation_Type"])
        final_count = clean_df.count()
        
        rows_dropped = initial_count - final_count

        print("\n=== Cleaned Data Sample ===")
        clean_df.show(5, truncate=False)
        clean_df.printSchema()

        print(f"\nWriting cleaned data to: {output_path}")
        clean_df.write.mode("overwrite").parquet(output_path)
        print(f"\nCleaned data written successfully to: {output_path}")

    except Exception as e:
        print(f"ERROR: An exception occurred during Spark processing.", file=sys.stderr)
        print(f"Exception details: {e}", file=sys.stderr)
    
    finally:
        spark.stop()
        print("Spark Session stopped.")
    
    # Force the script to exit if on Windows
    if sys.platform == "win32":
        print("Forcing script exit on Windows.")
        sys.exit(0)

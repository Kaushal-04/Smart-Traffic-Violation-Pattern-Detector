import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, coalesce, try_to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
        StructField("Severity", StringType(), True),
        StructField("Latitude", StringType(), True), 
        StructField("Longitude", StringType(), True) 
    ])

    try:
        df = spark.read.option("multiLine", "true").schema(schema).json(input_path)
        
    except Exception as e:
        print(f"ERROR: Could not read JSON file from {input_path}.", file=sys.stderr)
        spark.stop()
        raise

    print("\n=== Raw JSON Data Schema and Sample ===")
    df.printSchema()

    try:
        df = df.select([
            when(trim(col(c)) == "", None).otherwise(trim(col(c))).alias(c)
            for c in df.columns
        ])
        df = df.withColumn(
            "Timestamp",
            coalesce(
                try_to_timestamp(col("Timestamp"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("Timestamp"), lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            )
        )

        df = df.withColumn("Severity", col("Severity").cast(IntegerType()))
        df = df.withColumn("Latitude", col("Latitude").cast(DoubleType()))
        df = df.withColumn("Longitude", col("Longitude").cast(DoubleType()))
        df = df.withColumn("Location", 
                            when(col("Location").isNull(), lit("Unknown"))
                            .otherwise(col("Location"))
        )

        initial_count = df.count()
        clean_df = df.dropna(subset=["Violation_ID", "Violation_Type", "Timestamp"])
        final_count = clean_df.count()
        rows_dropped = initial_count - final_count

        valid_coord_count = clean_df.filter(col("Latitude").isNotNull() & col("Longitude").isNotNull()).count()
        
        print(f"Rows dropped due to missing key fields: {rows_dropped}")
        print(f"Total rows remaining: {final_count}")
        print(f"\n*** DEBUG: Rows with VALID Lat/Long: {valid_coord_count} ***\n")

        print("\n=== Cleaned Data Sample ===")
        clean_df.printSchema()

        print(f"\nWriting cleaned data to: {output_path}")
        clean_df.write.mode("overwrite").parquet(output_path)
        print(f"Cleaned data written successfully to: {output_path}")

    except Exception as e:
        print(f"ERROR: An exception occurred during Spark processing.", file=sys.stderr)
        raise
    
    finally:
        spark.stop()
        print("Spark Session stopped.")

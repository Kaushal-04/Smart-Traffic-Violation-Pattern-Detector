import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, date_format, count, desc
if sys.platform == "win32" and 'HADOOP_HOME' not in os.environ:
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'

def analyze_patterns(input_path, output_base_path, top_n=10):
    """
    Analyzes traffic violation patterns across time, type, and location.

    :param input_path: Path to the cleaned Parquet data.
    :param output_base_path: Base directory for storing aggregated Parquet results.
    :param top_n: Number of top locations to identify.
    """
    spark = SparkSession.builder \
        .appName("ViolationPatternAnalysis") \
        .master("local[*]") \
        .getOrCreate()
        
    print(f"Spark Session initialized. Reading data from {input_path}...")

    try:
        # Read the cleaned data
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"ERROR: Could not read Parquet file from {input_path}.", file=sys.stderr)
        raise

    print(f"Total records loaded: {df.count()}")
    
    # --- Time and Type-Based Aggregations ---
    print("\n=== Time and Type-Based Analysis ===")

    # Derive time-based features
    print("Deriving time-based features (hour, dayofweek, month, year)...")
    df_features = df.withColumn("Violation_Hour", hour(col("Timestamp"))) \
                    .withColumn("Violation_DayOfWeek", date_format(col("Timestamp"), "E")) \
                    .withColumn("Violation_Month", month(col("Timestamp"))) \
                    .withColumn("Violation_Year", year(col("Timestamp")))
    
    # 1. Total violations per hour of day
    print("Aggregating violations per hour...")
    hourly_counts = df_features.groupBy("Violation_Hour") \
                                .agg(count("*").alias("Total_Violations")) \
                                .orderBy("Violation_Hour")
    hourly_counts.write.mode("overwrite").parquet(f"{output_base_path}/hourly_counts.parquet")
    print(f"Wrote hourly_counts to {output_base_path}/hourly_counts.parquet")

    # 2. Total violations per day of week
    print("Aggregating violations per day of week...")
    dayofweek_counts = df_features.groupBy("Violation_DayOfWeek") \
                                  .agg(count("*").alias("Total_Violations")) \
                                  .orderBy(col("Total_Violations"), ascending=False)
    dayofweek_counts.write.mode("overwrite").parquet(f"{output_base_path}/dayofweek_counts.parquet")
    print(f"Wrote dayofweek_counts to {output_base_path}/dayofweek_counts.parquet")

    # 3. Total violations by type of offense
    print("Aggregating violations by type of offense...")
    type_counts = df.groupBy("Violation_Type") \
                    .agg(count("*").alias("Total_Violations")) \
                    .orderBy(desc("Total_Violations"))
    type_counts.write.mode("overwrite").parquet(f"{output_base_path}/type_counts.parquet")
    print(f"Wrote type_counts to {output_base_path}/type_counts.parquet")

    # 4. Cross-tab: Violation type × Hour of day
    print("Creating cross-tab (Violation Type x Hour)...")
    type_hour_crosstab = df_features.crosstab("Violation_Type", "Violation_Hour")
    type_hour_crosstab.write.mode("overwrite").parquet(f"{output_base_path}/type_hour_crosstab.parquet")
    print(f"Wrote type_hour_crosstab to {output_base_path}/type_hour_crosstab.parquet")


    print("\n=== Location-Based Analysis ===")
    # 1. Total violations per Location ID
    print("Aggregating violations per Location ID...")
    location_counts = df.groupBy("Location") \
                        .agg(count("*").alias("Total_Violations"))
    
    # 2. Top N locations with highest violation counts
    print(f"Finding Top {top_n} locations...")
    top_locations = location_counts.orderBy(desc("Total_Violations")) \
                                   .limit(top_n)
    location_counts.write.mode("overwrite").parquet(f"{output_base_path}/all_location_counts.parquet")
    top_locations.write.mode("overwrite").parquet(f"{output_base_path}/top_{top_n}_locations.parquet")
    
    print(f"Wrote all_location_counts to {output_base_path}/all_location_counts.parquet")
    print(f"Wrote top_{top_n}_locations to {output_base_path}/top_{top_n}_locations.parquet")

    spark.stop()
    print("\nSpark Session stopped. All aggregation results stored.")

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, date_format, count, desc, when, lit, floor, round as spark_round, concat, mean, stddev
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

if sys.platform == "win32" and 'HADOOP_HOME' not in os.environ:
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'

def analyze_patterns(input_path, output_base_path, top_n=10):
    """
    Analyzes traffic violation patterns across time, type, and location (standard aggregations).
    """
    spark = SparkSession.builder \
        .appName("ViolationPatternAnalysis") \
        .master("local[*]") \
        .getOrCreate()
        
    print(f"Spark Session initialized. Reading data from {input_path}...")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"ERROR: Could not read Parquet file from {input_path}.", file=sys.stderr)
        spark.stop()
        raise

    df_features = df.withColumn("Violation_Hour", hour(col("Timestamp"))) \
                      .withColumn("Violation_DayOfWeek", date_format(col("Timestamp"), "E")) 
    
    hourly_counts = df_features.groupBy("Violation_Hour") \
                               .agg(count("*").alias("Total_Violations")) \
                               .orderBy("Violation_Hour")
    hourly_counts.write.mode("overwrite").parquet(f"{output_base_path}/hourly_counts.parquet")

    dayofweek_counts = df_features.groupBy("Violation_DayOfWeek") \
                                  .agg(count("*").alias("Total_Violations")) 
    dayofweek_counts.write.mode("overwrite").parquet(f"{output_base_path}/dayofweek_counts.parquet")

    type_counts = df.groupBy("Violation_Type") \
                    .agg(count("*").alias("Total_Violations")) \
                    .orderBy(desc("Total_Violations"))
    type_counts.write.mode("overwrite").parquet(f"{output_base_path}/type_counts.parquet")

    location_counts = df.groupBy("Location") \
                        .agg(count("*").alias("Total_Violations"))
    top_locations = location_counts.orderBy(desc("Total_Violations")).limit(top_n)
    top_locations.write.mode("overwrite").parquet(f"{output_base_path}/top_{top_n}_locations.parquet")
    
    spark.stop()
    print("Standard analysis written.")


def run_advanced_analysis(input_path, output_base_path, k_clusters=5, stddev_multiplier=3.0):
    """
    Implements detailed time-based grouping and location hotspot identification.
    MODIFIED: Now ensures output files are created even with small datasets.
    """
    spark = SparkSession.builder \
        .appName("AdvancedViolationPatternAnalysis") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"ERROR: Could not read Parquet file from {input_path}.", file=sys.stderr)
        spark.stop()
        raise

    df_features = df.withColumn("Violation_Hour", hour(col("Timestamp")))

    df_features = df_features.withColumn(
        "Time_Window_3Hr",
        (floor(col("Violation_Hour") / 3) * 3).cast("string")
    )
    df_features = df_features.withColumn(
        "Day_Type",
        when(dayofweek(col("Timestamp")).isin(1, 7), lit("Weekend")).otherwise(lit("Weekday"))
    )
    
    type_3hr_crosstab = df_features.crosstab("Violation_Type", "Time_Window_3Hr")
    type_3hr_crosstab.write.mode("overwrite").parquet(f"{output_base_path}/type_3hr_crosstab.parquet")
    
    type_daytype_counts = df_features.groupBy("Violation_Type", "Day_Type") \
                                     .agg(count("*").alias("Total_Violations")) \
                                     .orderBy(desc("Total_Violations"))
    type_daytype_counts.write.mode("overwrite").parquet(f"{output_base_path}/type_daytype_counts.parquet")

    if all(c in df_features.columns for c in ["Latitude", "Longitude"]):
        df_geo = df_features.filter(col("Latitude").isNotNull() & col("Longitude").isNotNull())
        
        if df_geo.count() > 0:
            df_geo = df_geo.withColumn("Lat_Rounded", spark_round(col("Latitude"), 3))
            df_geo = df_geo.withColumn("Long_Rounded", spark_round(col("Longitude"), 3))
            df_geo = df_geo.withColumn(
                "Geo_Grid_Cell",
                concat(col("Lat_Rounded"), lit("_"), col("Long_Rounded"))
            )

            grid_counts = df_geo.groupBy("Geo_Grid_Cell") \
                                .agg(count("*").alias("Total_Violations"))
            grid_counts.write.mode("overwrite").parquet(f"{output_base_path}/geo_grid_counts.parquet")

            if grid_counts.count() > 10: 
                location_stats = grid_counts.select(
                    mean(col("Total_Violations")).alias("Mean_Violations"),
                    stddev(col("Total_Violations")).alias("StdDev_Violations")
                ).collect()[0]

                mean_v = location_stats["Mean_Violations"] or 0
                stddev_v = location_stats["StdDev_Violations"] or 0
                hotspot_threshold = mean_v + (stddev_multiplier * stddev_v) 

                significant_hotspots = grid_counts.filter(col("Total_Violations") >= hotspot_threshold) \
                                                  .orderBy(desc("Total_Violations"))
                print(f"Statistical Hotspots calculated. Threshold: {hotspot_threshold:.2f}")
            else:
                print("Small dataset detected: Treating all grid cells as hotspots.")
                significant_hotspots = grid_counts.orderBy(desc("Total_Violations"))

            significant_hotspots.write.mode("overwrite").parquet(f"{output_base_path}/significant_hotspots.parquet")

            if df_geo.count() >= k_clusters:
                assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol="features")
                data = assembler.transform(df_geo)
                kmeans = KMeans().setK(k_clusters).setSeed(1)
                model = kmeans.fit(data)
                predictions = model.transform(data)
                cluster_density = predictions.groupBy("prediction") \
                                             .agg(count("*").alias("Violation_Count")) \
                                             .orderBy(desc("Violation_Count"))
                cluster_density.write.mode("overwrite").parquet(f"{output_base_path}/kmeans_cluster_density.parquet")
                print(f"K-Means clustering completed for k={k_clusters}.")
            else:
                print(f"Skipping K-Means: Not enough data points ({df_geo.count()}) for {k_clusters} clusters.")
        
        else:
            print("Skipping location analysis: No valid geo-coordinates found.")
    else:
        print("\nSkipping all Location-Based Analysis: 'Latitude' or 'Longitude' columns not found.")
        
    spark.stop()
    print("Advanced analysis complete.")

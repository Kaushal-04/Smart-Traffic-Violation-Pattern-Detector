import os
import sys
from data_processing import process_traffic_data
from pattern_analysis import analyze_patterns 

# Set environment variables for local Spark execution
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

if __name__ == "__main__":
    input_path = "traffic_violations.json"
    cleaned_output_path = "cleaned_traffic_violations.parquet"
    analysis_output_base_path = "traffic_analysis_results"
    
    print(f"Starting Data Pipeline...")
    print("\n--- Data Cleaning ---")
    try:
        process_traffic_data(input_path, cleaned_output_path)
        print("(Cleaning) completed successfully.")
    except Exception as e:
        print(f"(Cleaning) failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Pattern Analysis & Aggregation ---")
    try:
        os.makedirs(analysis_output_base_path, exist_ok=True)
        analyze_patterns(cleaned_output_path, analysis_output_base_path, top_n=10)
        print("(Analysis) completed successfully.")
    except Exception as e:
        print(f"(Analysis) failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nData Pipeline finished successfully.")
    sys.exit(0)

import os
import sys
from data_processing import process_traffic_data

# Set environment variables for local Spark execution
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

if __name__ == "__main__":
    input_path = "traffic_violations.json"
    output_path = "cleaned_traffic_violations.parquet"
    
    print(f"Starting Spark job...")
    print(f"Input file:  {input_path}")
    print(f"Output file: {output_path}")

    try:
        process_traffic_data(input_path, output_path)
        print("\nSpark job completed successfully.")
    except Exception as e:
        print(f"\nSpark job failed with an exception: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nExiting script.")
    sys.exit(0)

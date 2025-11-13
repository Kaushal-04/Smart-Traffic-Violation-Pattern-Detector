import os
import sys
from data_processing import process_traffic_data
from pattern_analysis import analyze_patterns, run_advanced_analysis

from yolo_processor import detect_violations

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
if sys.platform == "win32" and 'HADOOP_HOME' not in os.environ:
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'  

if __name__ == "__main__":
    image_input = "test_traffic.jpg" 
    input_path = "traffic_violations.json"
    cleaned_output_path = "cleaned_traffic_violations.parquet"
    standard_analysis_path = "traffic_analysis_results"  
    advanced_analysis_path = "advanced_traffic_analysis"  
    
    print(f"Starting Data Pipeline...")
    print("\n--- YOLO Violation Detection (yolo_processor.py) ---")
    if not os.path.exists(image_input):
        print(f"Error: Input image '{image_input}' not found.", file=sys.stderr)
        print("Please add a traffic image named 'test_traffic.jpg' to the folder.", file=sys.stderr)
        sys.exit(1)
        
    try:
        detect_violations(image_input, input_path)
        print("(YOLO processing) completed successfully.")
    except Exception as e:
        print(f"(YOLO processing) failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Data Cleaning & Transformation (data_processing.py) ---")
    try:
        process_traffic_data(input_path, cleaned_output_path)
        print("(Cleaning) completed successfully.")
    except Exception as e:
        print(f"(Cleaning) failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\n--- Standard Pattern Analysis (pattern_analysis.py) ---")
    try:
        os.makedirs(standard_analysis_path, exist_ok=True)
        analyze_patterns(cleaned_output_path, standard_analysis_path, top_n=10)
        print("(Standard Analysis) completed successfully.")
    except Exception as e:
        print(f"(Standard Analysis) failed: {e}", file=sys.stderr)
        pass  

    print("\n--- Advanced Pattern Analysis & Hotspots (pattern_analysis.py) ---")
    try:
        os.makedirs(advanced_analysis_path, exist_ok=True)
        run_advanced_analysis(cleaned_output_path, advanced_analysis_path, k_clusters=5, stddev_multiplier=3.0)  
        print("(Advanced Analysis) completed successfully.")
    except Exception as e:
        print(f"(Advanced Analysis) failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nData Pipeline finished successfully.")
    print("\nTo view the dashboard, run the following command in your terminal:")
    print("streamlit run dashboard.py")
    sys.exit(0)

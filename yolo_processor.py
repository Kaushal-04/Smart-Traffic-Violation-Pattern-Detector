import cv2
import json
import uuid
import random
from datetime import datetime, timedelta
from ultralytics import YOLO

model = YOLO('yolov8n.pt') 

def detect_violations(image_path, output_json_path):
    """
    Runs YOLO detection on an image to get vehicle types, THEN 
    generates a LARGER, MORE VARIED simulated violation JSON file 
    for the Spark pipeline.
    """
    
    print(f"Processing image: {image_path} with YOLO to get vehicle types...")
    
    try:
        results = model(image_path)
    except Exception as e:
        print(f"Error: Could not process image. Ensure 'yolov8n.pt' model is downloaded.")
        print(f"Exception: {e}")
        return

    violation_types = [
        "Red Light", 
        "Lane Changing", 
        "Speeding",
        "Illegal U-Turn",
        "Parking Violation" 
    ]
    
    class_to_vehicle = {
        2: 'Car',        
        3: 'Motorcycle',
        5: 'Bus',        
        7: 'Truck'      
    }

    detected_vehicle_types = []
    for result in results:
        for box in result.boxes:
            class_id = int(box.cls[0])
            if class_id in class_to_vehicle:
                detected_vehicle_types.append(class_to_vehicle[class_id])

    if not detected_vehicle_types:
        print("No usable vehicles (car, motorcycle, bus, truck) detected in the image.")
        print("Simulating with default types.")
        detected_vehicle_types = ['Car', 'Truck', 'Motorcycle'] 

    print(f"Detected vehicle types to use for simulation: {list(set(detected_vehicle_types))}")

    location_details = [
        {"Location": "Intersection_A", "Latitude": "40.7128", "Longitude": "-74.0060"},
        {"Location": "Intersection_B", "Latitude": "40.7359", "Longitude": "-73.9752"},
        {"Location": "Intersection_C", "Latitude": "40.7589", "Longitude": "-73.9851"},
        {"Location": "Highway_Exit_1", "Latitude": "40.6892", "Longitude": "-74.0445"},
    ]

    violations_list = []
    num_violations_to_sim = 500 
    base_time = datetime.now()

    print(f"Generating {num_violations_to_sim} simulated violation records...")

    for _ in range(num_violations_to_sim):
        random_days_ago = random.randint(0, 30)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        sim_time = (base_time - timedelta(days=random_days_ago)) \
                        .replace(hour=random_hour, minute=random_minute, second=0, microsecond=0)

        loc = random.choice(location_details)

        record = {
            "Violation_ID": str(uuid.uuid4()),
            "Timestamp": sim_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Violation_Type": random.choice(violation_types),
            "Vehicle_Type": random.choice(detected_vehicle_types),
            "Severity": str(random.randint(1, 5)),
            "Location": loc["Location"],
            "Latitude": loc["Latitude"],
            "Longitude": loc["Longitude"]
        }
        violations_list.append(record)
                    
    try:
        with open(output_json_path, 'w') as f:
            json.dump(violations_list, f, indent=4)
        print(f"Successfully generated {len(violations_list)} violation(s) to {output_json_path}")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")

if __name__ == "__main__":
    test_image = "test_traffic.jpg" 
    test_output = "traffic_violations.json"
    
    import os
    if not os.path.exists(test_image):
        print(f"Error: Test image '{test_image}' not found.")
    else:
        detect_violations(test_image, test_output)

import pandas as pd
import argparse
from datetime import datetime, timedelta
import os
def extract_trips(parquet_file, output_dir):
    # Read the Parquet file into a pandas DataFrame
    df = pd.read_parquet(raw_data.parquet)

    # Sort the DataFrame by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Initialize variables for trip identification
    current_unit = None
    current_trip_number = 0
    current_trip_start_time = None

    for index, row in df.iterrows():
        if current_unit is None or current_unit != row['unit']:
            # Start a new trip for a new unit
            current_unit = row['unit']
            current_trip_number = 0
            current_trip_start_time = row['timestamp']
        else:
            # Check if a new trip should start based on the time difference
            time_difference = row['timestamp'] - current_trip_start_time
            if time_difference > timedelta(hours=1):
                current_trip_number += 1
                current_trip_start_time = row['timestamp']

        # Create or append to the CSV file for the current trip
        output_file = os.path.join(output_dir, f"{current_unit}_{current_trip_number}.csv")
        row_data = {
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'timestamp': row['timestamp'].isoformat(),
        }
        pd.DataFrame([row_data]).to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
        
     
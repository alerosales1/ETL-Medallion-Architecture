import os
import pandas as pd

input_dir = '01-bronze-raw'  # Read CSV files from here
output_dir = '02-silver-cleaned'  # Parquet files will be saved here

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

list_files = os.listdir(input_dir)

for file in os.listdir(input_dir):
    input_path = os.path.join(input_dir, file)
    name, ext = os.path.splitext(file)
    output_path = os.path.join(output_dir, f"{name}.parquet")

    if ext.lower() == '.csv':  # Read CSV file
        df = pd.read_csv(input_path)
    elif ext.lower() == '.json':  # Read JSON file
        try:
            df = pd.read_json(input_path)  # Try reading as standard JSON
        except ValueError as e:  # If it fails, try reading as JSON lines
            df = pd.read_json(input_path, lines=True)
    else:
        # Skip unsupported formats
        print(f"Unsupported file format: {file}")
        continue

    # Convert list type columns to string to allow drop_duplicates
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ','.join(
                map(str, x)) if isinstance(x, list) else x)

    df = df.drop_duplicates().reset_index(drop=True)  # Remove duplicates

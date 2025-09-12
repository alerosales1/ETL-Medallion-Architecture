import os
import pandas as pd

input_dir = '01-bronze-raw'  # Read CSV files from here
output_dir = '02-silver-cleaned'  # Parquet files will be saved here

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

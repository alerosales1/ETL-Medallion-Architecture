import os
import pandas as pd


class NormalizeData:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    # Convert list-type columns to comma-separated strings
    def convert_columns_to_string(self, df):
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: ','.join(
                    map(str, x)) if isinstance(x, list) else x)
        return df

    # Load DataFrame from CSV or JSON file
    def load_df_from_file(self, file, ext):
        input_path = os.path.join(self.input_dir, file)

        if ext.lower() == '.csv':
            df = pd.read_csv(input_path)
        elif ext.lower() == '.json':
            # Try read as object list
            try:
                df = pd.read_json(input_path)
            except ValueError:
                # Fallback to line-delimited JSON
                df = pd.read_json(input_path, lines=True)
        return df

    def normalize_data(self):
        for file in os.listdir(self.input_dir):
            name, ext = os.path.splitext(file)
            output_path = os.path.join(
                self.output_dir, f'{name}.parquet')

            df = self.load_df_from_file(file, ext)

            df = self.convert_columns_to_string(df)
            df = df.drop_duplicates().reset_index(drop=True)

            df.to_parquet(output_path, index=False)
            print(f"Normalized files saved to {self.output_dir}")


if __name__ == "__main__":
    normalize_data = NormalizeData(
        input_dir='01-bronze-raw', output_dir='02-silver-validated')
    normalize_data.normalize_data()


input_dir = '01-bronze-raw'  # Read CSV files from here
output_dir = '02-silver-validated'  # Parquet files will be saved here

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

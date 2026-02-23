import duckdb
import pandas as pd
import os

# Environment setup
if os.environ.get("WORKSPACE_BUCKET"):
    bucket = os.environ.get("WORKSPACE_BUCKET")
else:
    bucket = "bucket_placeholder"

engine = duckdb.connect("/tmp/dbt.duckdb")


def execute(query):
    """
    Connect to duckdb, execute a query and format as a DataFrame with headers.
    """
    result = engine.execute(query)
    try:
        # duckdb result provides a direct fetch to pandas DataFrame
        return result.fetchdf()
    except Exception:
        # fallback to manual construction if fetchdf is not available
        return pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])

def convert_csv_to_utf8(input_file_path, output_filepath, delimiter, encoding):
    df = pd.read_csv(input_file_path, encoding=encoding, delimiter=delimiter, quoting=3)
    df.to_csv(output_filepath, index=False, encoding='utf-8')
    print(f"Converted CSV saved to {output_filepath}")

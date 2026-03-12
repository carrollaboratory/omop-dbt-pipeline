#!/usr/bin/env python
# +
# ASSIGN THESE VARIABLES
study_id = 'consort_gira'
study_data_dir = "../_study_data/consort_gira/eMERGE_6_Month_Data_External_Release" # From the repo root dir.
harmonized_bucket_dir = 'eMERGE_6_Month_Data_External_Release'
tgt_schema = 'main_main'
pipeline_repo = 'omop_dbt_pipeline'

# The prefix of the harmonized data in the pipeline. ex 'emerge_consort_gira_stb_observation' --> 'emerge_consort_gira_stb_'
export_prefix = 'emerge_consort_gira_stb_' 

# A list of target/export pipeline table names. ex 'emerge_consort_gira_stb_observation' --> 'observation'
# The notebook will write the data to csvs with these names. ex 'emerge_consort_gira_stb_observation' --> 'observation.csv'
export_tables = ['care_site',
                 'condition_occurrence',
                 'device_exposure',
                 'drug_exposure',
                 'observation',
                 'person',
                 'procedure_occurrence',
                 'visit_occurrence',
                 'measurement']

# -

from pathlib import Path
import pandas as pd
import subprocess
import os
import yaml
import duckdb

# +
# Environment setup
if os.environ.get("WORKSPACE_BUCKET"):
    bucket = os.environ.get("WORKSPACE_BUCKET")
else:
    bucket = "bucket_placeholder"
    
engine = duckdb.connect("/tmp/dbt.duckdb")

# +
repo_home_dir = Path.cwd().parent
output_study_dir = repo_home_dir / f"../output_data/{study_id}"
study_data_dir = repo_home_dir / study_data_dir
bucket_study_dir = f'{bucket}/harmonized/{harmonized_bucket_dir}'

paths = {
    "repo_home_dir": repo_home_dir,
    "output_study_dir": output_study_dir,
    "src_data_dir": study_data_dir,
    "bucket_study_dir": f'{bucket_study_dir}',
}


# -

def read_file(filepath):
    
    file_handlers = {
        ".yaml": lambda: yaml.safe_load(open(filepath, "r")),
        ".yml": lambda: yaml.safe_load(open(filepath, "r")),
        ".csv": lambda: pd.read_csv(filepath, header=0),
        ".xlsx": lambda: pd.read_excel(filepath, header=0),
        ".sql": Path(filepath).read_text
    }

    file_ext = os.path.splitext(filepath)[-1].lower()

    if file_ext not in file_handlers:
        raise ValueError(f"Unsupported file type: {file_ext}")

    data = file_handlers[file_ext]()

    return data


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


# # COPY tables from duckdb into pipeline/output_data/{study_id}/{table}.csv

for t in export_tables:
    input_tablename = f'{tgt_schema}.{export_prefix}{t}'
    output_filename = f'{paths["output_study_dir"]}/{t}.csv'
    
    t = engine.execute(
        f"COPY (SELECT * FROM {input_tablename}) TO '{output_filename}' (HEADER, DELIMITER ',')"
    ).fetchall()
    print(f'Printing {input_tablename} to {output_filename}.')

# for t in export_tables:
#     input_tablename = f'{tgt_schema}.{export_prefix}{t}'
#     output_filename = f'{paths["output_study_dir"]}/{t}.parquet'
#     
#     t = engine.execute(
#         f"COPY (SELECT * FROM {input_tablename}) TO '{output_filename}' (FORMAT PARQUET)"
#     ).fetchall()
#     print(f'Printing {input_tablename} to {output_filename}.')

# # COPY tables from pipeline/output_data/{study_id} into the workspace bucket

# +
print(f'gsutil cp -r {output_study_dir}/ {bucket_study_dir}')

#     subprocess.run(
#         ["gsutil", "cp", "-r", output_study_dir , bucket_study_dir], check=True
#     )
# -

if __name__ == "__main__":
    main()

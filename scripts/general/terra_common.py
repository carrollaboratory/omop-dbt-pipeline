# +
import pandas as pd
import os
from pathlib import Path
from dbt_pipeline_utils.scripts.helpers.general import get_paths

from scripts.general.common import bucket, engine


# -

def get_terra_paths(study_id):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    """
    
    repo_home_dir = Path(__file__).parent.parent.parent
    validation_yml_path = (
        repo_home_dir / "data" / study_id / f"{study_id}_validation.yaml"
    )
    output_dir = repo_home_dir.parent / "output_data"
    output_study_dir = output_dir / study_id
    output_validation_dir = output_study_dir / "validation"
    seeds_dir = repo_home_dir / "seeds"
    notebook_dir = repo_home_dir / "notebooks"
    bucket_study_dir = f"{bucket}/{study_id}"

    return {
        "repo_home_dir": repo_home_dir,
        "validation_yml_path": validation_yml_path,
        "output_dir": output_dir,
        "output_study_dir": output_study_dir,
        "output_validation_dir": output_validation_dir,
        "seeds_dir": seeds_dir,
        "notebook_dir": notebook_dir,
        "bucket_study_dir": bucket_study_dir,
    }


def get_all_paths(
    study_id=None, dbt_repo=None, org_id=None, tgt_model_id=None, src_data_path=None
):
    """
    Creates one dictionary of frequently used paths.

    The Terra paths are specific to directories required for Terra development.
    The pipeline_utils paths cover the paths within the project repository.
    """

    paths = {}
    paths.update(get_terra_paths(study_id))
    paths.update(
        get_paths(study_id, org_id, tgt_model_id, src_data_path)
    )  # pipeline_utils paths

    return paths


# pipeline helpers
def create_file_dict(table, count):
    file_list = []
    for i in range(count):
        if i == 0:
            file = f'{table}_{"0" * 12}.csv'
        else:
            file = f'{table}_{"0" * (12 - len(str(i)))}{i}.csv'
        file_list.append(file)

    return {table: file_list}

def copy_data_to_bucket(bucket_study_dir, file_list, input_dir):
    for file in file_list:
        # !gsutil cp {input_dir} {bucket_study_dir}/{file}
        print(f'INFO: Copied {file} to the bucket') 

# Read and concatenate all files
def read_and_concat_files(file_list, input_dir, output_dir):
    dfs = [pd.read_csv(f'{input_dir}/{file}') for file in file_list] 
    combined_subject = pd.concat(dfs, ignore_index=True)
    combined_subject.to_csv(output_dir, index=False)

def remove_file(file_list, d_dir):
    for file in file_list:
        file_path = os.path.join(d_dir, file)
        try:
            os.remove(file_path)
            print(f'INFO: Processed: {file}')
        except FileNotFoundError:
            print(f'WARNING: File not found: {file}')
        except Exception as e:
            print(f'ERROR: Could not remove {file} due to {e}')
    return

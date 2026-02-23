#!/usr/bin/env python
"""
Will clean a column of data consisting of keywords or codes separated by bars. Use search_dragon directly
if no cleaning is required, or if there are a small number of search items.

eturns a file for the
harmonizer to review. Then uses search_dragon to return the top match to the cleaned keywords.

Files will be created at `data/tools/code_metadata`

Example usage. Use the help command to get info on all available arguments. 
./scripts/get_code_metadata.py -df 'data/static/example_data/test_col_clean.csv' -c 'test_codes' -o 'HP,HPO'

NOTE: search_dragon can be used directly if no cleaning is necessary.
"""

# +
import argparse
import subprocess
import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from dbt_pipeline_utils import logger

from scripts.general.terra_common import get_all_paths


# -

def clean_codes(codes, curies):
    """
    Cleans the input codes by ensuring proper prefixes and removing invalid characters.

    Args:
        codes (str): Input string of codes to clean.
        curies (list): List of prefixes to ensure codes are formatted correctly.

    Returns:
        str: Cleaned string of codes.
    """
    codes = str(codes) if not pd.isnull(codes) else ""

    for c in curies:  # Ensure codes are separated by the |
        codes = codes.replace(c, f"|{c}").replace(c, f"{c}:").replace(f"{c}::", f"{c}:")
    codes = codes.replace(" ", "")  # Whitespace
    codes = codes.replace("''", "").replace('"', "").replace("\"","")  # Quotations
    codes = codes.replace("Ê", "")  # Sp characters
    codes = codes.replace("|||", "|").replace("||", "|")  # Multiple bars
    codes = codes.strip("'")  # Leading and trailing bars
    codes = codes.strip("|")  # Leading and trailing bars

    return codes


def is_valid_format(code):
    """
    Validates if the code matches the expected format: PREFIX:VALUE.

    Args:
        code (str): Single code to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    
    return bool(re.fullmatch(r"[A-Z]+:\d+", code))


def create_flag_column(codes):
    # Check if each code matches the valid format
    codes = str(codes) if not pd.isnull(codes) else ""
    return [is_valid_format(code) for code in codes.split("|") if code.strip()]


def run_dragon_search(keywords, ontologies, output_filepath):
    """
    Executes the dragon_search command with the provided keywords, ontology prefixes, and output filepath.

    Args:
        keywords (str): Pipe-separated string of keywords to search.
        ontologies (str): Comma-separated list of ontology prefixes.
        output_filepath (str): Path to save the output file.
    """
    quoted_keywords = f'"{keywords}"'
    quoted_ontologies = f'"{ontologies}"'

    command = [
        "dragon_search",
        "-ak",
        quoted_keywords,
        "-o",
        quoted_ontologies,
        "-f",
        f"{output_filepath}",
    ]

    try:
        logger.info(f"Running dragon_search with command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.info(f"dragon_search output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running dragon_search: {e.stderr}")
        print(f"Error running command: {' '.join(command)}")
        print(f"Command failed with exit status {e.returncode}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Get metadata for a code using the available locutus OntologyAPI connection."
    )

    parser.add_argument(
        "-df",
        "--data_file",
        required=True,
        help="File containing the codes requiring metadata. Format: 'path/to/datafile.csv'",
    )
    parser.add_argument(
        "-c",
        "--column",
        required=True,
        help="Column name containing the codes requiring metadata. The utils can do some amount of cleaning.",
    )
    parser.add_argument(
        "-o",
        "--ontologies",
        required=True,
        help="Comma-separated list of ontology prefixes (e.g., 'HP,HPO').",
    )
    parser.add_argument(
        "-r",
        "--results_per_page",
        required=False,
        default=1,
        help="How many pages should the API return per request. Both APIs give a response for each code.",
    )
    parser.add_argument(
        "-s",
        "--start_index",
        required=False,
        default=1,
        help="Which page should be returned. Note: most likely you don't want to use this.",
    )

    args = parser.parse_args()

    # Parse ontology prefixes into a list
    curies = args.ontologies.split(",")

    # Read input data
    df = pd.read_csv(args.data_file)

    # Clean the column
    df["cleaned_col"] = df[args.column].apply(lambda x: clean_codes(x, curies) if pd.notnull(x) else x)
    # Create a flag column for validation
    df["cleaned_col"] = df["cleaned_col"].drop_duplicates(keep='first')
    df = df[df["cleaned_col"].notna()]
    df["correct_format"] = df["cleaned_col"].apply(create_flag_column)

    # Set filepaths
    paths = get_all_paths(
        "placeholder",
        "anvil_dbt_project",
        "anvil",
    )
    code_dir = paths["static_data_dir"] / "../tools/code_metadata"
    cleaned_codes_path = code_dir / "col_clean.csv"
    search_output_path = (
        code_dir / f"annotations_{datetime.now().strftime('%Y%m%d')}.csv"
    )

    # Save cleaned data to file
    t = df[["cleaned_col", "correct_format"]].dropna(subset=["cleaned_col"])    
    t.to_csv(cleaned_codes_path, index=False)

    # Catch all codes that might not be cleaned properly.
    questionable = df[df["correct_format"].apply(lambda x: not all(x))]
    if len(questionable) >= 1:
        logger.warning(f": {questionable[['cleaned_col', 'correct_format']]}")

    logger.info(f"Cleaned column is returned in {cleaned_codes_path}")

    # Join all unique, clean codes, into a single string to use as the api search input.
    unique_codes=set()

    for row in df["cleaned_col"]:
        unique_codes.update(row.split('|'))
    all_keywords = '|'.join(unique_codes) 
    logger.debug(all_keywords)

    run_dragon_search(all_keywords, args.ontologies, search_output_path)


if __name__ == "__main__":
    main()

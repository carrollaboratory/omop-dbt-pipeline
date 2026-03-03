#!/usr/bin/env python
# ```
# Tool to query tables in the duckdb database.
# Create new cells for study specific analysis/validation. 
# ```

import duckdb
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
        return result.fetchdf()
    except Exception:
        return pd.DataFrame(result.fetchall(), columns=[col[0] for col in result.description])


table = execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
)
print(table)

table = execute(
    "PRAGMA table_info('main.table')"
)
print(f"Table Info\n {table}\n")

table = execute(
    "SELECT * FROM main.table LIMIT 10"
)
print(f"FTD Demographics\n {table}\n")

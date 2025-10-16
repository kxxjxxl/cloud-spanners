import argparse
import json
import pandas as pd
from google.cloud import spanner


def load_csv(file_path: str, dtype_path: str = None, chunksize: int = None):
    """
    Load a CSV file into a pandas DataFrame or generator of DataFrames (if chunksize is set).
    Optionally apply a JSON file defining column data types.
    """
    dtypes = None
    if dtype_path:
        with open(dtype_path, "r") as f:
            dtypes = json.load(f)

    return pd.read_csv(file_path, dtype=dtypes, chunksize=chunksize)


def connect_spanner(instance_id: str, database_id: str):
    """
    Establish a connection to a Google Cloud Spanner database.
    """
    client = spanner.Client()
    instance = client.instance(instance_id)
    return instance.database(database_id)


def insert_dataframe(db, table_name: str, df: pd.DataFrame):
    """
    Insert a pandas DataFrame into a Spanner table using batch DML.
    """
    with db.batch() as batch:
        batch.insert(
            table=table_name,
            columns=list(df.columns),
            values=df.values.tolist()
        )


def process_csv_to_spanner(
    instance_id: str,
    database_id: str,
    table_name: str,
    file_path: str,
    dtype_path: str = None,
    chunksize: int = None
):
    """
    Load CSV data (with optional schema) and insert into a Spanner table, in batches if specified.
    """
    db = connect_spanner(instance_id, database_id)

    # If chunksize is not set or invalid, load the full file at once
    if not chunksize or chunksize <= 0:
        print("[INFO] Importing file in a single batch...")
        df = load_csv(file_path, dtype_path)
        insert_dataframe(db, table_name, df)
        print("[SUCCESS] Data inserted successfully.")
        return

    # Otherwise, process the file in chunks
    print(f"[INFO] Importing file in chunks of {chunksize} rows...")
    for i, chunk in enumerate(load_csv(file_path, dtype_path, chunksize=chunksize), start=1):
        print(f"[BATCH {i}] Inserting {len(chunk)} rows...")
        insert_dataframe(db, table_name, chunk)
    print("[SUCCESS] All chunks inserted successfully.")


def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Import CSV data into Google Cloud Spanner.")
    parser.add_argument('--instance_id', required=True, help='Cloud Spanner instance ID.')
    parser.add_argument('--database_id', required=True, help='Cloud Spanner database ID.')
    parser.add_argument('--table_id', required=True, help='Target Cloud Spanner table name.')
    parser.add_argument('--file_path', required=True, help='Path to the CSV file.')
    parser.add_argument('--format_path', help='Path to JSON file with column data types.')
    parser.add_argument('--chunksize', type=int, default=-1, help='Number of rows per batch insert.')

    return parser.parse_args()


def main():
    args = parse_args()

    process_csv_to_spanner(
        instance_id=args.instance_id,
        database_id=args.database_id,
        table_name=args.table_id,
        file_path=args.file_path,
        dtype_path=args.format_path,
        chunksize=args.chunksize
    )


if __name__ == "__main__":
    main()

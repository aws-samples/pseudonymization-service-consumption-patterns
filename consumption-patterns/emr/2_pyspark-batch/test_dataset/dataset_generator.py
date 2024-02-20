import pandas as pd
from datetime import datetime
from random import randrange
import pyarrow as pa
import pyarrow.parquet as pq
import sys


"""
	Title: Parquet Dataset Generator
	Description: 
        This code is responsible to generate a parquet dataset of a given size.
	Example usage: python3 dataset_generator.py <DESIRED_DATASET_SIZE_MB>
"""


file_size_mb = int(sys.argv[1])

MAX_DEPTS = 50  # Maximum department ID
MAX_RANGE = 10000  # Maximum VIN range


# Function to generate rows for the DataFrame
def generate_row():
    return {
        "dept_id": str(randrange(MAX_DEPTS)),
        "vin": str(randrange(MAX_RANGE)),
        "example_ts": datetime.now().isoformat(),
    }


# Initialize the file size and empty list to hold batches of rows
row_batches = []
current_file_size_mb = 0

while current_file_size_mb < file_size_mb:
    print(current_file_size_mb)
    # Generate a batch of rows
    batch = [generate_row() for _ in range(10)]
    # Create a DataFrame from the batch
    df = pd.DataFrame(batch)
    # Convert the DataFrame to Parquet format
    table = pa.Table.from_pandas(df)
    # Write the batch to a Parquet file
    with pa.BufferOutputStream() as sink:
        pq.write_table(table, sink)
        batch_size_mb = len(sink.getvalue()) / (1024 * 1024)

    # Update the file size
    current_file_size_mb += batch_size_mb
    # Append the batch to the list for later writing
    row_batches.append(batch)

# Concatenate all batches and export the DataFrame to a Parquet file
df = pd.concat([pd.DataFrame(batch) for batch in row_batches], ignore_index=True)
df.to_parquet(f"dataset_{file_size_mb}.parquet", index=False)

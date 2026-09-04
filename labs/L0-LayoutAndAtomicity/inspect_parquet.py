import pyarrow.parquet as pq
from pathlib import Path

# Change this to one of your actual downloaded files
FILE = next(
    Path.home()
    .glob("Desktop/FuckDE/datasets/raw/nyc-taxi/*.parquet")
)

print("=" * 80)
print("FILE")
print("=" * 80)
print(FILE)
print(f"File size: {FILE.stat().st_size / 1024 / 1024:.2f} MB")

# Open the Parquet file
parquet = pq.ParquetFile(FILE)

metadata = parquet.metadata

print("\n" + "=" * 80)
print("FILE LEVEL METADATA")
print("=" * 80)

print("Total rows:", metadata.num_rows)
print("Total columns:", metadata.num_columns)
print("Row groups:", metadata.num_row_groups)
print("Parquet format version:", metadata.format_version)

print("\n" + "=" * 80)
print("SCHEMA")
print("=" * 80)

print(parquet.schema)

print("\n" + "=" * 80)
print("ROW GROUPS")
print("=" * 80)

for rg_index in range(metadata.num_row_groups):
    row_group = metadata.row_group(rg_index)

    print(f"\nROW GROUP {rg_index}")
    print(f"Rows: {row_group.num_rows}")
    print(f"Compressed size: {row_group.total_byte_size / 1024 / 1024:.2f} MB")

    for col_index in range(row_group.num_columns):
        column = row_group.column(col_index)

        print(f"""
  Column: {column.path_in_schema}
    Physical type: {column.physical_type}
    Compression: {column.compression}
    Encodings: {column.encodings}
    Compressed size: {column.total_compressed_size:,} bytes
    Uncompressed size: {column.total_uncompressed_size:,} bytes
""")

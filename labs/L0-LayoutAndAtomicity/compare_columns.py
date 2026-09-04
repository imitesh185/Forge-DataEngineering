import duckdb
import pyarrow.parquet as pq
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FILE = next(
    (PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("*.parquet")
)

# Low → medium → potentially high cardinality
COLUMNS = [
    "VendorID",
    "PULocationID",
    "fare_amount",
    "tpep_pickup_datetime"
]

parquet = pq.ParquetFile(FILE)
metadata = parquet.metadata

con = duckdb.connect()
file_sql = str(FILE).replace("'", "''")

total_rows = metadata.num_rows

print("=" * 110)
print("CARDINALITY VS PARQUET PHYSICAL STORAGE")
print("=" * 110)
print(f"File: {FILE.name}")
print(f"Total rows: {total_rows:,}\n")

for target_column in COLUMNS:

    print("\n" + "=" * 110)
    print(f"COLUMN: {target_column}")
    print("=" * 110)

    # ---------------------------------------------------------
    # Logical characteristics
    # ---------------------------------------------------------

    distinct_count = con.execute(f"""
        SELECT COUNT(DISTINCT "{target_column}")
        FROM '{file_sql}'
    """).fetchone()[0]

    null_count = con.execute(f"""
        SELECT COUNT(*)
        FROM '{file_sql}'
        WHERE "{target_column}" IS NULL
    """).fetchone()[0]

    cardinality = distinct_count / total_rows

    print("\nLOGICAL DATA")
    print(f"Total rows:       {total_rows:,}")
    print(f"Distinct values:  {distinct_count:,}")
    print(f"Null values:      {null_count:,}")
    print(f"Cardinality ratio:{cardinality:.10f}")

    # ---------------------------------------------------------
    # Physical Parquet characteristics
    # ---------------------------------------------------------

    compressed = 0
    uncompressed = 0
    encodings = set()
    compressions = set()
    dictionary_pages = 0

    print("\nROW GROUP PHYSICAL METADATA")

    for rg_index in range(metadata.num_row_groups):

        rg = metadata.row_group(rg_index)

        for col_index in range(rg.num_columns):

            column = rg.column(col_index)

            if column.path_in_schema == target_column:

                compressed += column.total_compressed_size
                uncompressed += column.total_uncompressed_size

                encodings.update(column.encodings)
                compressions.add(str(column.compression))

                if column.has_dictionary_page:
                    dictionary_pages += 1

                print(
                    f"Row Group {rg_index}: "
                    f"compressed={column.total_compressed_size:,} bytes | "
                    f"uncompressed={column.total_uncompressed_size:,} bytes | "
                    f"dictionary={column.has_dictionary_page}"
                )

    print("\nTOTAL PHYSICAL STORAGE")
    print(f"Physical encodings: {sorted(encodings)}")
    print(f"Compression:        {sorted(compressions)}")
    print(f"Dictionary pages:   {dictionary_pages}/{metadata.num_row_groups}")
    print(f"Uncompressed:       {uncompressed:,} bytes")
    print(f"Compressed:         {compressed:,} bytes")

    if compressed > 0:
        print(
            f"Compression ratio:  "
            f"{uncompressed / compressed:.2f}x"
        )

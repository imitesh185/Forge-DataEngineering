import pyarrow.parquet as pq
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FILE = next(
    (PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("*.parquet")
)

COLUMN = "VendorID"

print("=" * 80)
print("FILE")
print("=" * 80)
print(FILE)

# ------------------------------------------------------------
# 1. Physical metadata
# ------------------------------------------------------------

parquet = pq.ParquetFile(FILE)
metadata = parquet.metadata

print("\n" + "=" * 80)
print("PHYSICAL METADATA: VendorID")
print("=" * 80)

total_compressed = 0
total_uncompressed = 0

for rg_index in range(metadata.num_row_groups):
    rg = metadata.row_group(rg_index)

    for col_index in range(rg.num_columns):
        column = rg.column(col_index)

        if column.path_in_schema == COLUMN:

            total_compressed += column.total_compressed_size
            total_uncompressed += column.total_uncompressed_size

            print(f"\nROW GROUP {rg_index}")
            print(f"Rows: {rg.num_rows}")
            print(f"Physical type: {column.physical_type}")
            print(f"Encodings: {column.encodings}")
            print(f"Compression: {column.compression}")
            print(f"Has dictionary page: {column.has_dictionary_page}")
            print(f"Dictionary page offset: {column.dictionary_page_offset}")
            print(f"Data page offset: {column.data_page_offset}")
            print(f"Compressed size: {column.total_compressed_size:,} bytes")
            print(f"Uncompressed size: {column.total_uncompressed_size:,} bytes")

print("\n" + "=" * 80)
print("TOTAL VendorID COLUMN STORAGE")
print("=" * 80)

print(f"Total compressed:   {total_compressed:,} bytes")
print(f"Total uncompressed: {total_uncompressed:,} bytes")

if total_compressed:
    print(
        f"Compression ratio: "
        f"{total_uncompressed / total_compressed:.2f}x"
    )


# ------------------------------------------------------------
# 2. Logical values and cardinality
# ------------------------------------------------------------

con = duckdb.connect()

file_sql = str(FILE).replace("'", "''")

print("\n" + "=" * 80)
print("LOGICAL VALUES")
print("=" * 80)

result = con.execute(f"""
    SELECT
        VendorID,
        COUNT(*) AS count
    FROM '{file_sql}'
    GROUP BY VendorID
    ORDER BY VendorID
""").fetchall()

for vendor_id, count in result:
    print(f"VendorID = {vendor_id!r}  ->  {count:,} rows")


total_rows = con.execute(f"""
    SELECT COUNT(*)
    FROM '{file_sql}'
""").fetchone()[0]

distinct_count = con.execute(f"""
    SELECT COUNT(DISTINCT VendorID)
    FROM '{file_sql}'
""").fetchone()[0]

print("\n" + "=" * 80)
print("CARDINALITY")
print("=" * 80)

print(f"Total rows:      {total_rows:,}")
print(f"Distinct values: {distinct_count:,}")
print(f"Cardinality:     {distinct_count / total_rows:.10f}")


# ------------------------------------------------------------
# 3. Raw INT32 comparison
# ------------------------------------------------------------

raw_int32_bytes = total_rows * 4

print("\n" + "=" * 80)
print("WHAT IF EVERY VALUE WERE STORED AS RAW INT32?")
print("=" * 80)

print(f"Rows × 4 bytes: {raw_int32_bytes:,} bytes")
print(f"Actual uncompressed column storage: {total_uncompressed:,} bytes")
print(f"Actual compressed column storage:   {total_compressed:,} bytes")

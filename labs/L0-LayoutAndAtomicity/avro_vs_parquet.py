from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq

from fastavro import writer, reader, parse_schema


# =========================================================
# PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE = next(
    (PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("*.parquet")
)

WORK = PROJECT_ROOT / "datasets/raw/format-choice"
WORK.mkdir(parents=True, exist_ok=True)

PARQUET = WORK / "data.parquet"
AVRO = WORK / "data.avro"


# =========================================================
# LOAD DATA
# =========================================================

table = pq.read_table(SOURCE).slice(0, 1_000_000)

print(f"Rows: {table.num_rows}")
print(f"Columns: {table.num_columns}")


# =========================================================
# AVRO SCHEMA
# =========================================================

def avro_type(field):

    if pa.types.is_integer(field.type):
        return "long"

    if pa.types.is_floating(field.type):
        return "double"

    if pa.types.is_large_string(field.type) or pa.types.is_string(field.type):
        return "string"

    if pa.types.is_timestamp(field.type):
        return {
            "type": "long",
            "logicalType": "timestamp-micros"
        }

    raise TypeError(
        f"Unsupported type: {field.name} -> {field.type}"
    )


schema = {
    "type": "record",
    "name": "TaxiTrip",
    "fields": [
        {
            "name": field.name,
            "type": avro_type(field),
        }
        for field in table.schema
    ],
}

parsed_schema = parse_schema(schema)


# =========================================================
# CONVERT ARROW → AVRO RECORDS
# =========================================================

records = table.to_pylist()

for record in records:

    for field in table.schema:

        if pa.types.is_timestamp(field.type):

            value = record[field.name]

            if value is not None:
                record[field.name] = int(
                    value.timestamp() * 1_000_000
                )


# =========================================================
# 1. WRITE PARQUET
# =========================================================

if PARQUET.exists():
    PARQUET.unlink()

start = time.perf_counter()

pq.write_table(
    table,
    PARQUET,
    compression="zstd"
)

parquet_write = time.perf_counter() - start


# =========================================================
# 2. WRITE AVRO
# =========================================================

if AVRO.exists():
    AVRO.unlink()

start = time.perf_counter()

with open(AVRO, "wb") as f:

    writer(
        f,
        parsed_schema,
        records
    )

avro_write = time.perf_counter() - start


# =========================================================
# 3. SIZE
# =========================================================

parquet_size = PARQUET.stat().st_size
avro_size = AVRO.stat().st_size


# =========================================================
# 4. APPEND ONE RECORD
# =========================================================

one_record = records[0]


# -------------------------
# Parquet
# -------------------------

start = time.perf_counter()

existing = pq.read_table(PARQUET)

one_arrow_table = pa.Table.from_pylist(
    [one_record],
    schema=existing.schema
)

combined = pa.concat_tables(
    [existing, one_arrow_table]
)

pq.write_table(
    combined,
    PARQUET,
    compression="zstd"
)

parquet_append = time.perf_counter() - start


# -------------------------
# Avro
# -------------------------

start = time.perf_counter()

with open(AVRO, "rb") as f:
    existing_records = list(reader(f))

existing_records.append(one_record)

with open(AVRO, "wb") as f:

    writer(
        f,
        parsed_schema,
        existing_records
    )

avro_append = time.perf_counter() - start


# =========================================================
# 5. SELECT ONE COLUMN
# =========================================================

COLUMN = "fare_amount"


# -------------------------
# Parquet
# -------------------------

start = time.perf_counter()

pq.read_table(
    PARQUET,
    columns=[COLUMN]
)

parquet_select = time.perf_counter() - start


# -------------------------
# Avro
# -------------------------

start = time.perf_counter()

with open(AVRO, "rb") as f:

    values = [
        record[COLUMN]
        for record in reader(f)
    ]

avro_select = time.perf_counter() - start


# =========================================================
# RESULTS
# =========================================================

print()
print("=" * 55)
print("              AVRO vs PARQUET")
print("=" * 55)

print(
    f"\n{'Metric':<25}"
    f"{'Parquet':>14}"
    f"{'Avro':>14}"
)

print("-" * 55)

print(
    f"{'Size (MB)':<25}"
    f"{parquet_size / 1024**2:>14.2f}"
    f"{avro_size / 1024**2:>14.2f}"
)

print(
    f"{'Append 1 row (sec)':<25}"
    f"{parquet_append:>14.4f}"
    f"{avro_append:>14.4f}"
)

print(
    f"{'Select 1 column (sec)':<25}"
    f"{parquet_select:>14.4f}"
    f"{avro_select:>14.4f}"
)

print("-" * 55)
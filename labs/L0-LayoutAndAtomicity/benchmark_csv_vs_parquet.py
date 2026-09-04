import duckdb
import time
import statistics
from pathlib import Path

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PARQUET_FILE = next(
    (PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("*.parquet")
)

OUTPUT_DIR = PROJECT_ROOT / "datasets/benchmark"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_FILE = OUTPUT_DIR / "yellow_tripdata_2024-01.csv"

RUNS = 7

# ------------------------------------------------------------
# DUCKDB
# ------------------------------------------------------------

con = duckdb.connect()

parquet_path = str(PARQUET_FILE).replace("'", "''")
csv_path = str(CSV_FILE).replace("'", "''")

# ------------------------------------------------------------
# CREATE CSV ONCE
# ------------------------------------------------------------

if not CSV_FILE.exists():

    print("=" * 80)
    print("CREATING EQUIVALENT CSV")
    print("=" * 80)

    con.execute(f"""
        COPY (
            SELECT *
            FROM '{parquet_path}'
        )
        TO '{csv_path}'
        (
            HEADER,
            DELIMITER ','
        )
    """)

    print(f"Created: {CSV_FILE}")

else:
    print(f"CSV already exists: {CSV_FILE}")


# ------------------------------------------------------------
# FILE SIZES
# ------------------------------------------------------------

print("\n" + "=" * 80)
print("FILE SIZES")
print("=" * 80)

print(
    f"Parquet: "
    f"{PARQUET_FILE.stat().st_size / 1024 / 1024:.2f} MB"
)

print(
    f"CSV:     "
    f"{CSV_FILE.stat().st_size / 1024 / 1024:.2f} MB"
)


# ------------------------------------------------------------
# GET COLUMN NAMES
# ------------------------------------------------------------

columns = con.execute(f"""
    DESCRIBE
    SELECT *
    FROM '{parquet_path}'
""").fetchall()

column_names = [row[0] for row in columns]

print("\nColumns:")

for i, column in enumerate(column_names, start=1):
    print(f"{i:2}. {column}")


# ------------------------------------------------------------
# BENCHMARK FUNCTION
# ------------------------------------------------------------

def benchmark(query, runs=RUNS):

    times = []

    # Warm-up
    con.execute(query).fetchall()

    for _ in range(runs):

        start = time.perf_counter()

        con.execute(query).fetchall()

        end = time.perf_counter()

        times.append(end - start)

    return {
        "median": statistics.median(times),
        "minimum": min(times),
        "maximum": max(times),
        "all": times
    }


# ------------------------------------------------------------
# PROJECTION LEVELS
# ------------------------------------------------------------

projection_counts = []

for count in [1, 2, 5, 10, len(column_names)]:

    if count <= len(column_names):
        projection_counts.append(count)

# Remove duplicates while preserving order
projection_counts = list(dict.fromkeys(projection_counts))


# ------------------------------------------------------------
# RUN BENCHMARK
# ------------------------------------------------------------

print("\n" + "=" * 100)
print("CSV VS PARQUET BENCHMARK")
print("=" * 100)

results = []

for count in projection_counts:

    selected_columns = column_names[:count]

    projection = ", ".join(
        f'"{column}"'
        for column in selected_columns
    )

    print("\n" + "-" * 100)
    print(f"PROJECTION: {count} COLUMN(S)")
    print("-" * 100)

    parquet_query = f"""
        SELECT {projection}
        FROM '{parquet_path}'
    """

    csv_query = f"""
        SELECT {projection}
        FROM read_csv_auto('{csv_path}')
    """

    print("\nBenchmarking Parquet...")

    parquet_result = benchmark(parquet_query)

    print(
        f"Parquet median: "
        f"{parquet_result['median']:.6f} seconds"
    )

    print("Benchmarking CSV...")

    csv_result = benchmark(csv_query)

    print(
        f"CSV median:     "
        f"{csv_result['median']:.6f} seconds"
    )

    winner = (
        "PARQUET"
        if parquet_result["median"] < csv_result["median"]
        else "CSV"
    )

    ratio = (
        csv_result["median"]
        / parquet_result["median"]
    )

    results.append({
        "columns": count,
        "parquet": parquet_result["median"],
        "csv": csv_result["median"],
        "winner": winner,
        "ratio": ratio
    })


# ------------------------------------------------------------
# SUMMARY
# ------------------------------------------------------------

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)

print(
    f"{'Columns':<10}"
    f"{'Parquet':<15}"
    f"{'CSV':<15}"
    f"{'Winner':<15}"
    f"{'CSV/Parquet':<15}"
)

print("-" * 100)

for result in results:

    print(
        f"{result['columns']:<10}"
        f"{result['parquet']:<15.6f}"
        f"{result['csv']:<15.6f}"
        f"{result['winner']:<15}"
        f"{result['ratio']:<15.2f}"
    )

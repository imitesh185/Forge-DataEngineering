from pathlib import Path
import duckdb
import time
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT = next(
    (PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("yellow_tripdata_2024-01.parquet")
)

OUTPUT_DIR = PROJECT_ROOT / "datasets/raw/nyc-taxi/codec-test"
OUTPUT_DIR.mkdir(exist_ok=True)

CODECS = [
    "UNCOMPRESSED",
    "SNAPPY",
    "ZSTD",
    "GZIP",
]

QUERY = """
    SELECT
        *
    FROM read_parquet(?)
    WHERE trip_distance > 10
"""


con = duckdb.connect()


# ============================================================
# 1. CREATE ONE FILE PER CODEC
# ============================================================

print("=" * 80)
print("CREATING FILES")
print("=" * 80)

for codec in CODECS:

    output = OUTPUT_DIR / f"nyc_{codec.lower()}.parquet"

    if output.exists():
        output.unlink()

    start = time.perf_counter()

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{INPUT}')
        )
        TO '{output}'
        (
            FORMAT PARQUET,
            COMPRESSION {codec}
        )
    """)

    write_time = time.perf_counter() - start

    size_mb = output.stat().st_size / (1024 * 1024)

    print(
        f"{codec:12} "
        f"size={size_mb:8.2f} MB "
        f"write={write_time:8.3f} s"
    )


# ============================================================
# 2. WARM UP DUCKDB
# ============================================================

print("\n" + "=" * 80)
print("WARMUP")
print("=" * 80)

for codec in CODECS:

    output = OUTPUT_DIR / f"nyc_{codec.lower()}.parquet"

    con.execute(QUERY, [str(output)]).fetchall()


# ============================================================
# 3. MEASURE READ / DECODE TIME
# ============================================================

print("\n" + "=" * 80)
print("READ / DECODE")
print("=" * 80)

results = []

REPEATS = 10

for codec in CODECS:

    output = OUTPUT_DIR / f"nyc_{codec.lower()}.parquet"

    times = []

    for _ in range(REPEATS):

        start = time.perf_counter()

        result = con.execute(
            QUERY,
            [str(output)]
        ).fetchall()

        elapsed = time.perf_counter() - start

        times.append(elapsed)

    avg_time = sum(times) / len(times)

    size_mb = output.stat().st_size / (1024 * 1024)

    results.append({
        "codec": codec,
        "size_mb": size_mb,
        "write_seconds": None,
        "read_seconds": avg_time,
        "rows": len(result),
    })

    print(
        f"{codec:12} "
        f"read={avg_time:8.4f} s "
        f"rows={len(result)}"
    )


# ============================================================
# 4. PRINT FINAL TABLE
# ============================================================

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print(
    f"{'Codec':<15}"
    f"{'Size MB':>12}"
    f"{'Read sec':>12}"
)

print("-" * 45)

for row in results:

    print(
        f"{row['codec']:<15}"
        f"{row['size_mb']:>12.2f}"
        f"{row['read_seconds']:>12.4f}"
    )
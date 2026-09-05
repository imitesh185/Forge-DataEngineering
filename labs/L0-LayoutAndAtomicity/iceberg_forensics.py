from pathlib import Path
import json
import shutil
import time

import pyarrow as pa
from pyiceberg.catalog import load_catalog


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

WORK = PROJECT_ROOT / "datasets" / "raw" / "iceberg-b5"

WAREHOUSE = WORK / "warehouse"
CATALOG_DB = WORK / "catalog.db"

TABLE_NAME = "b5_demo"


# ============================================================
# CLEAN START
# ============================================================

if WORK.exists():
    shutil.rmtree(WORK)

WAREHOUSE.mkdir(parents=True)

print("=" * 70)
print("B5 — ICEBERG FORENSICS")
print("=" * 70)

print(f"\nWarehouse : {WAREHOUSE}")
print(f"Catalog   : {CATALOG_DB}")


# ============================================================
# CREATE LOCAL CATALOG
# ============================================================

catalog = load_catalog(
    "b5",
    **{
        "type": "sql",
        "uri": f"sqlite:///{CATALOG_DB}",
        "warehouse": f"file://{WAREHOUSE}",
    },
)

catalog.create_namespace("demo")

# Small table intentionally.
# We want to inspect the physical structure easily.
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("amount", pa.float64()),
])

table = catalog.create_table(
    f"demo.{TABLE_NAME}",
    schema=schema,
)


# ============================================================
# HELPER: SHOW FILESYSTEM
# ============================================================

def show_files(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    for path in sorted(WORK.rglob("*")):
        if path.is_file():
            size = path.stat().st_size
            print(f"{path.relative_to(WORK)}    ({size:,} bytes)")


# ============================================================
# HELPER: SNAPSHOT INFORMATION
# ============================================================

def show_snapshot(label):
    snapshot = table.current_snapshot()

    print("\n" + "-" * 70)
    print(label)
    print("-" * 70)

    if snapshot is None:
        print("No snapshot")
        return

    print(f"snapshot_id       : {snapshot.snapshot_id}")
    print(f"sequence_number   : {snapshot.sequence_number}")
    print(f"operation         : {snapshot.summary.operation}")
    print(f"manifest_list     : {snapshot.manifest_list}")


# ============================================================
# INSERT #1
# ============================================================

print("\n\n### INSERT 1")

table.append(
    pa.table({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "amount": [100.0, 200.0],
    })
)

show_snapshot("AFTER INSERT 1")
show_files("FILES AFTER INSERT 1")


# ============================================================
# INSERT #2
# ============================================================

print("\n\n### INSERT 2")

table.append(
    pa.table({
        "id": [3, 4],
        "name": ["Charlie", "David"],
        "amount": [300.0, 400.0],
    })
)

show_snapshot("AFTER INSERT 2")
show_files("FILES AFTER INSERT 2")


# ============================================================
# INSERT #3
# ============================================================

print("\n\n### INSERT 3")

table.append(
    pa.table({
        "id": [5, 6],
        "name": ["Eve", "Frank"],
        "amount": [500.0, 600.0],
    })
)

show_snapshot("AFTER INSERT 3")
show_files("FILES AFTER INSERT 3")


# ============================================================
# CURRENT DATA
# ============================================================

print("\n\n### CURRENT TABLE")

print(table.scan().to_arrow())


# ============================================================
# CAPTURE CURRENT DATA FILES
# ============================================================

before_data_files = {
    p: p.stat().st_mtime_ns
    for p in WAREHOUSE.rglob("*.parquet")
}

print("\n\nData files BEFORE DELETE:")

for path in before_data_files:
    print(" ", path.relative_to(WORK))


# ============================================================
# SHOW METADATA JSON
# ============================================================

print("\n\n" + "=" * 70)
print("CURRENT METADATA JSON")
print("=" * 70)

metadata_path = Path(table.metadata_location.replace("file://", ""))

print(f"\nMetadata file:\n{metadata_path.relative_to(WORK)}")

with open(metadata_path) as f:
    metadata = json.load(f)

print("\nTop-level metadata keys:")
for key in metadata:
    print(" ", key)

print("\nCurrent snapshot ID:")
print(" ", metadata.get("current-snapshot-id"))

print("\nSnapshots:")
for snapshot in metadata.get("snapshots", []):
    print(
        f"  snapshot_id={snapshot['snapshot-id']} "
        f"sequence={snapshot['sequence-number']} "
        f"manifest_list={snapshot['manifest-list']}"
    )


# ============================================================
# DELETE ONE ROW
# ============================================================

print("\n\n" + "=" * 70)
print("DELETE ONE ROW")
print("=" * 70)

print("\nDeleting:")
print("id = 3")

old_snapshot = table.current_snapshot().snapshot_id

table.delete("id = 3")

new_snapshot = table.current_snapshot().snapshot_id

print("\nOLD snapshot:")
print(" ", old_snapshot)

print("\nNEW snapshot:")
print(" ", new_snapshot)


# ============================================================
# SHOW TABLE AFTER DELETE
# ============================================================

print("\n\n### TABLE AFTER DELETE")

print(table.scan().to_arrow())


# ============================================================
# SHOW NEW SNAPSHOT
# ============================================================

show_snapshot("AFTER DELETE")


# ============================================================
# COMPARE PARQUET FILES
# ============================================================

after_data_files = {
    p: p.stat().st_mtime_ns
    for p in WAREHOUSE.rglob("*.parquet")
}

print("\n\n" + "=" * 70)
print("PARQUET FILE COMPARISON")
print("=" * 70)

print("\nExisting before AND after:")

for path in before_data_files.keys() & after_data_files.keys():
    changed = before_data_files[path] != after_data_files[path]

    print(
        f"  {path.relative_to(WORK)}"
        f"   changed={changed}"
    )

print("\nRemoved after delete:")

for path in before_data_files.keys() - after_data_files.keys():
    print(" ", path.relative_to(WORK))

print("\nCreated after delete:")

for path in after_data_files.keys() - before_data_files.keys():
    print(" ", path.relative_to(WORK))


# ============================================================
# FINAL FILESYSTEM
# ============================================================

show_files("FINAL ICEBERG TABLE")


print("\n\n" + "=" * 70)
print("DONE")
print("=" * 70)
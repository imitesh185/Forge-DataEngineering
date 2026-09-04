import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FILE = next(
	(PROJECT_ROOT / "datasets/raw/nyc-taxi").glob("*.parquet")
)

con = duckdb.connect()

print(f"\nFILE: {FILE}\n")

#Query 1:
print("="*80)
print("Query 1 : SELECT *")
print("="*80)

print(
	con.execute(f"""
		EXPLAIN ANALYZE
		SELECT * FROM '{FILE}'
	""").fetchall()[0][1]
)
print("\n")
#Query 2:
print("="*80)
print("Query 2 : SELECT distinct VendorID and Passenger Count")
print("="*80)

print(       
        con.execute(f"""
                EXPLAIN ANALYZE
                SELECT distinct VendorID, passenger_count FROM '{FILE}' where passenger_count > 3
        """).fetchall()[0][1]
)

import sqlite3
from soil_reading_incremental_ingest import main as incremental_ingest  # already updated

DB_FILE = "data/soil_sensor_data.db"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# 1️ Capture last 10 rows (full data)
cur.execute("""
    SELECT ID, Local_Time, Site_ID, Probe_ID, Soil_Value, Probe_Measure, Unit, json_featuretype
    FROM raw_soil_data
    ORDER BY ID DESC
    LIMIT 10
""")
deleted_rows = cur.fetchall()

print("Rows to delete (captured):")
for row in deleted_rows:
    print(row)

# 2️ Delete last 10 rows
cur.execute("""
    DELETE FROM raw_soil_data
    WHERE ID IN ({})
""".format(','.join(str(r[0]) for r in deleted_rows)))
conn.commit()
print(f"\nDeleted {len(deleted_rows)} rows to simulate missing data.")

# 3️ Run incremental ingestion
print("\nRunning incremental ingestion...")
incremental_ingest()

# 4️ Fetch restored rows using unique keys (Site_ID, Probe_ID, Local_Time)
restored_rows = []
for r in deleted_rows:
    cur.execute("""
        SELECT ID, Local_Time, Site_ID, Probe_ID, Soil_Value, Probe_Measure, Unit, json_featuretype
        FROM raw_soil_data
        WHERE Site_ID=? AND Probe_ID=? AND Local_Time=?
    """, (r[2], r[3], r[1]))
    restored_rows.extend(cur.fetchall())

print("\nRestored rows (inserted by incremental ingestion):")
for row in restored_rows:
    print(row)

# 5️ Compare deleted vs restored
if sorted(deleted_rows) == sorted(restored_rows):
    print("\n Test passed: All deleted rows were restored correctly!")
else:
    print("\n Test failed: Restored rows do not match deleted rows.")

conn.close()

from dotenv import load_dotenv
load_dotenv()
import os

for _pg_bin in (r"C:\Program Files\PostgreSQL\18\bin",):
    if os.path.isdir(_pg_bin):
        os.add_dll_directory(_pg_bin)
        break
import psycopg

conn = psycopg.connect(os.environ["AUTOMETRIC_DB_URL"])
cur = conn.cursor()
cur.execute("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema='l1_silver'")
print("hypertables:", cur.fetchone()[0])   # harus 5
cur.execute("SELECT count(*) FROM l1_silver.unified_post")
print("post rows:", cur.fetchone()[0])
conn.close()
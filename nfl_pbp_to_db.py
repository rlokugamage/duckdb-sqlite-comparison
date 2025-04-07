import os
import pandas as pd
import sqlite3
import duckdb
import timeit

files = []
for parquet_file in os.listdir("./parquet_files"):
    files.append(pd.read_parquet(f"./parquet_files/{parquet_file}"))
df_sqlite = pd.concat(files, ignore_index=True)
df_duckdb = df_sqlite.copy(deep=True).reset_index().rename(columns={"index": "idx"})

print(f"Dataframe mem size (gb): {round(df_sqlite.memory_usage(index=True).sum() / 1_000_000_000, 2)}")

duckdb_conn = duckdb.connect("./dbs/nfl_pbp.duckdb")
sqlite3_conn = sqlite3.connect("./dbs/nfl_pbp.db")

def load_duckdb():
    duckdb_conn.sql("CREATE TABLE pbp AS SELECT * FROM df_duckdb")


def load_sqlite():
    df_sqlite.to_sql("pbp", con=sqlite3_conn, index_label="idx")

print("Process parquet files into duckdb")
load_duckdb_time = timeit.repeat(
    stmt=lambda: load_duckdb(),
    setup=lambda: duckdb_conn.execute("DROP TABLE IF EXISTS pbp"),
    repeat=5,
    number=1,
)
print("Process parquet files into sqlite3")
load_sqlite_time = timeit.repeat(
    stmt=load_sqlite,
    setup=lambda: sqlite3_conn.execute("DROP TABLE IF EXISTS pbp"),
    repeat=5,
    number=1,
)
print("Done")

duckdb_conn.close()
sqlite3_conn.close()
print(f"load_duckdb took on avg {sum(load_duckdb_time)/5:.2f} seconds")
print(f"load_sqlite took on avg {sum(load_sqlite_time)/5:.2f} seconds")

md_table = (
f"""# Results

## Timer
| Method | Time (seconds) |
| ------ | -------------- |
| duckdb | {sum(load_duckdb_time)/5:.2f} |
| sqlite | {sum(load_sqlite_time)/5:.2f} |
""")
result_file = open("./results/nfl_pbp_to_db.md", 'w+')
result_file.write(md_table)
result_file.close()

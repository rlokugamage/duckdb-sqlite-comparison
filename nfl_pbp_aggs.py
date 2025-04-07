import sqlite3
import timeit
from dataclasses import dataclass

import duckdb


duckdb_conn = duckdb.connect("dbs/nfl_pbp.duckdb")
sqlite_conn = sqlite3.connect("dbs/nfl_pbp.db")

@dataclass
class RowResults():
    duckdb_rows: list[tuple]
    sqlite_rows: list[tuple]

    def __iter__(self):
        return iter(zip(self.duckdb_rows, self.sqlite_rows))


row_results = RowResults(duckdb_rows=[], sqlite_rows=[])

def duckdb_avg():
    row_results.duckdb_rows = duckdb_conn.execute(
        "select home_team, avg(two_point_attempt) from pbp group by home_team order by home_team"
    ).fetchall()

def sqlite_avg():
    row_results.sqlite_rows = sqlite_conn.execute(
        "select home_team, avg(two_point_attempt) from pbp group by home_team order by home_team"
    ).fetchall()


print("Processing duckdb aggregate")
duckdb_avg_time = timeit.timeit(
    stmt=duckdb_avg,
    number=10
)
print("Processing sqlite aggregate")
sqlite_avg_time = timeit.timeit(
    stmt=sqlite_avg,
    number=10
)
print("Done")

print(f"duckdb_avg took on avg {duckdb_avg_time:.2f} seconds")
print(f"sqlite_avg took on avg {sqlite_avg_time:.2f} seconds")

duckdb_table = (
"""
## Duckdb
| home_team | avg(two_point_attempt) |
| --------- | ---------------------- |
""")

sqlite_table = (
"""
## Sqlite
| home_team | avg(two_point_attempt) |
| --------- | ---------------------- |
""")

for duckdb_row, sqlite_row in row_results:
    duckdb_table = duckdb_table + f"| {duckdb_row[0]} | {duckdb_row[1]:.8f} |" + "\n"
    sqlite_table = sqlite_table + f"| {sqlite_row[0]} | {sqlite_row[1]:.8f} |" + "\n"

md_table = (
f"""# Results

## Timer
| Method | Time (seconds) |
| ------ | -------------- |
| duckdb | {duckdb_avg_time:.2f} |
| sqlite | {sqlite_avg_time:.2f} |
""")
result_file = open("./results/nfl_pbp_aggs.md", 'w+')
result_file.write(md_table + duckdb_table + sqlite_table)
result_file.close()

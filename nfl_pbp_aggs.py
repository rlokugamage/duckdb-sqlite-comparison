import sqlite3
import timeit
from dataclasses import dataclass
from statistics import mean

import duckdb
from numpy.ma.core import repeat
from rich.console import Console
from rich.progress import (
    Progress,
    TaskID,
    TextColumn,
    SpinnerColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn
)

class TaskSetup:
    init: bool
    progress: Progress
    name: str
    total: int
    task: TaskID

    def __init__(self, name: str, *_, progress: Progress, total: int):
        self.init = True
        self.progress = progress
        self.name = name
        self.total = total

    def __enter__(self):
        self.task = self.progress.add_task(self.name, total=self.total)
        def setup():
            if self.init:
                self.init = False
                return
            self.progress.advance(self.task)
        return setup

    def __exit__(self, exc_type, exc_val, exc_tb):
       self.progress.advance(self.task)


@dataclass
class RowResults():
    duckdb_rows: list[tuple]
    sqlite_rows: list[tuple]

    def __iter__(self):
        return iter(zip(self.duckdb_rows, self.sqlite_rows))


console = Console()

duckdb_conn = duckdb.connect("dbs/nfl_pbp.duckdb")
sqlite_conn = sqlite3.connect("dbs/nfl_pbp.db")

row_results = RowResults(duckdb_rows=[], sqlite_rows=[])

def duckdb_avg():
    row_results.duckdb_rows = duckdb_conn.execute(
        "select home_team, avg(two_point_attempt) from pbp group by home_team order by home_team"
    ).fetchall()

def sqlite_avg():
    row_results.sqlite_rows = sqlite_conn.execute(
        "select home_team, avg(two_point_attempt) from pbp group by home_team order by home_team"
    ).fetchall()

console.print("Starting Aggregate Tasks")
with Progress(TextColumn("[progress.description]{task.description}"),
              SpinnerColumn(),
              BarColumn(),
              TaskProgressColumn(),
              TimeElapsedColumn(),
              console=console) as progress:
    with TaskSetup("Duckdb", progress=progress, total=100) as duckdb_setup:
        duckdb_avg_time = timeit.repeat(
            stmt=duckdb_avg,
            setup=duckdb_setup,
            repeat=100,
            number=1
        )

    with TaskSetup("Sqlite", progress=progress, total=100) as sqlite_setup:
        sqlite_avg_time = timeit.repeat(
            stmt=sqlite_avg,
            setup=sqlite_setup,
            repeat=100,
            number=1
        )

duckdb_conn.close()
sqlite_conn.close()
console.print("In 100 loops:")
console.print(f"- duckdb took on avg {mean(duckdb_avg_time):.2f} seconds")
console.print(f"- sqlite took on avg {mean(sqlite_avg_time):.2f} seconds")

with console.status("Writing results to nfl_pbp_aggs.md"):
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
    | duckdb | {mean(duckdb_avg_time):.2f} |
    | sqlite | {mean(sqlite_avg_time):.2f} |
    """)
    result_file = open("./results/nfl_pbp_aggs.md", 'w+')
    result_file.write(md_table + duckdb_table + sqlite_table)
    result_file.close()

import os
from sqlite3 import Connection
from statistics import mean

import pandas as pd
import sqlite3
import duckdb
import timeit

from duckdb.duckdb import DuckDBPyConnection
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TextColumn, TaskID
)

console = Console()

class TaskSetup:
    init: bool
    progress: Progress
    name: str
    total: int
    task: TaskID
    conn: DuckDBPyConnection | Connection

    def __init__(self, name: str, *_, progress: Progress, total: int, conn: DuckDBPyConnection | Connection):
        self.init = True
        self.progress = progress
        self.name = name
        self.total = total
        self.conn = conn

    def __enter__(self):
        self.task = self.progress.add_task(self.name, total=self.total)
        def setup():
            self.conn.execute("DROP TABLE IF EXISTS pbp")
            if self.init:
                self.init = False
                return
            self.progress.advance(self.task)
        return setup

    def __exit__(self, exc_type, exc_val, exc_tb):
       self.progress.advance(self.task)


files = []
with console.status("Loading Parquet Files..."):
    for parquet_file in os.listdir("./parquet_files"):
        if parquet_file.endswith(".md"):
            continue
        files.append(pd.read_parquet(f"./parquet_files/{parquet_file}"))
    df_sqlite = pd.concat(files, ignore_index=True)
    df_duckdb = df_sqlite.copy(deep=True).reset_index().rename(columns={"index": "idx"})

console.print(f"Dataframe mem size: {round(df_sqlite.memory_usage(index=True).sum() / 1_000_000_000, 2)} gb")

duckdb_conn = duckdb.connect("./dbs/nfl_pbp.duckdb")
sqlite_conn = sqlite3.connect("./dbs/nfl_pbp.db")

def load_duckdb():
    duckdb_conn.sql("CREATE TABLE pbp AS SELECT * FROM df_duckdb")


def load_sqlite():
    df_sqlite.to_sql("pbp", con=sqlite_conn, index_label="idx")

console.print("Starting DB Load tasks")
with Progress(TextColumn("[progress.description]{task.description}"),
              SpinnerColumn(),
              BarColumn(),
              TaskProgressColumn(),
              TimeElapsedColumn(),
              console=console) as progress:
    with TaskSetup("Duckdb", progress=progress, total=5, conn=duckdb_conn) as duckdb_setup:
        load_duckdb_time = timeit.repeat(
            stmt=load_duckdb,
            setup=duckdb_setup,
            repeat=5,
            number=1,
        )

    with TaskSetup("Sqlite", progress=progress, total=5, conn=sqlite_conn) as sqlite_setup:
        load_sqlite_time = timeit.repeat(
            stmt=load_sqlite,
            setup=sqlite_setup,
            repeat=5,
            number=1,
        )


duckdb_conn.close()
sqlite_conn.close()
console.print("In 5 loops:")
console.print(f"- duckdb took on avg {mean(load_duckdb_time):.2f} seconds")
console.print(f"- sqlite took on avg {mean(load_sqlite_time):.2f} seconds")

with console.status("Writing results to nfl_pbp_to_db.md"):
    md_table = (
    f"""# Results

    ## Timer
    | Method | Time (seconds) |
    | ------ | -------------- |
    | duckdb | {mean(load_duckdb_time):.2f} |
    | sqlite | {mean(load_sqlite_time):.2f} |
    """)
    result_file = open("./results/nfl_pbp_to_db.md", 'w+')
    result_file.write(md_table)
    result_file.close()

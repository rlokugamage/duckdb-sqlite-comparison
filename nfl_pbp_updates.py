import random
import sqlite3
import timeit
from dataclasses import dataclass
from sqlite3 import Connection
from statistics import mean

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from rich.console import Console
from rich.progress import (
    Progress,
    TaskID,
    TextColumn, SpinnerColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
)


@dataclass
class Scores:
    home_score: int
    away_score: int
    index: int
    prev_scores: list

    def __init__(self):
        self.home_score = random.randrange(100)
        self.away_score = random.randrange(100)
        self.index = random.randrange(1230855)
        self.prev_scores = []

    @property
    def result(self) -> int:
        return self.home_score - self.away_score

    @property
    def total(self) -> int:
        return self.home_score + self.away_score

    def __iter__(self):
        yield self.index
        yield self.home_score
        yield self.away_score
        yield self.result
        yield self.total

    def to_md(self):
        return '| ' + ' | '.join(map(str, list(self))) + ' |'

    def reset(self):
        self.prev_scores.append(self.to_md())
        self.home_score = random.randrange(100)
        self.away_score = random.randrange(100)
        self.index = random.randrange(1230855)


@dataclass
class DuckDbScores(Scores):
    def __init__(self):
        super().__init__()


@dataclass
class SqliteScores(Scores):
    def __init__(self):
        super().__init__()


class TaskSetup:
    init: bool
    progress: Progress
    name: str
    total: int
    task: TaskID
    scores: Scores

    def __init__(self, name: str, *_, progress: Progress, total: int, scores: Scores):
        self.init = True
        self.progress = progress
        self.name = name
        self.total = total
        self.scores = scores

    def __enter__(self):
        self.task = self.progress.add_task(self.name, total=self.total)
        def setup():
            if self.init:
                self.init = False
                return
            self.scores.reset()
            self.progress.advance(self.task)
        return setup

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.advance(self.task)
        self.scores.reset()


console = Console()

duckdb_conn = duckdb.connect("dbs/nfl_pbp.duckdb")
sqlite_conn = sqlite3.connect("dbs/nfl_pbp.db")

duckdb_scores = DuckDbScores()
sqlite_scores = SqliteScores()


def incremental_update(conn: DuckDBPyConnection | Connection, scores: Scores):
    conn.execute(f"UPDATE pbp "
                 f"SET "
                 f"total_home_score = {scores.home_score}, "
                 f"total_away_score = {scores.away_score}, "
                 f"result = {scores.result}, "
                 f"total = {scores.total} "
                 f"WHERE idx = {scores.index}")


def batch_update(conn: DuckDBPyConnection | Connection):
   conn.execute(
       "UPDATE pbp "
       "SET "
       "total_home_score = 10, "
       "total_away_score = 20, "
       "result = -10, "
       "total = 30 "
       "WHERE home_team = 'NE'")


console.print("Starting Incremental Update Tasks")
with Progress(TextColumn("[progress.description]{task.description}"),
              SpinnerColumn(),
              BarColumn(),
              TaskProgressColumn(),
              TimeElapsedColumn(),
              console=console) as progress:
    with TaskSetup("Duckdb", progress=progress, total=10000, scores=duckdb_scores) as duckdb_setup:
        duckdb_update_time = timeit.repeat(
            stmt=lambda : incremental_update(duckdb_conn, duckdb_scores),
            setup=duckdb_setup,
            repeat=10000,
            number=1
        )

    with TaskSetup("Sqlite", progress=progress, total=10000, scores=duckdb_scores) as sqlite_setup:
        sqlite_update_time = timeit.repeat(
            stmt=lambda : incremental_update(conn=sqlite_conn, scores=sqlite_scores),
            setup=sqlite_setup,
            repeat=10000,
            number=1
        )

console.print("in 10000 loops:")
console.print(f"- duckdb took on avg {mean(duckdb_update_time) * 1000:.2f} milliseconds")
console.print(f"- sqlite took on avg {mean(sqlite_update_time) * 1000:.2f} milliseconds")


console.print("Starting Batch Update Tasks")
with Progress(TextColumn("[progress.description]{task.description}"),
              SpinnerColumn(),
              BarColumn(),
              TaskProgressColumn(),
              TimeElapsedColumn(),
              console=console) as progress:
    with TaskSetup("Duckdb", progress=progress, total=100, scores=duckdb_scores) as duckdb_setup:
        duckdb_batch_update_time = timeit.repeat(
            stmt=lambda : batch_update(conn=duckdb_conn),
            setup=duckdb_setup,
            repeat=100,
            number=1
        )

    with TaskSetup("Sqlite", progress=progress, total=100, scores=duckdb_scores) as sqlite_setup:
        sqlite_batch_update_time = timeit.repeat(
            stmt=lambda : batch_update(sqlite_conn),
            setup=sqlite_setup,
            repeat=100,
            number=1
        )

console.print("In 100 loops:")
console.print(f"- duckdb took on avg {mean(duckdb_batch_update_time):.2f} seconds")
console.print(f"- sqlite took on avg {mean(sqlite_batch_update_time):.2f} seconds")

with console.status("Writing results to nfl_pbp_updates.md"):
    duckdb_table = (
    """
    ## Duckdb
    | index | home_score | away_score | result | total |
    | ----- | ---------- | ---------- | ------ | ----- |
    """)

    sqlite_table = (
    """
    ## Sqlite
    | index | home_score | away_score | result | total |
    | ----- | ---------- | ---------- | ------ | ----- |
    """)

    duckdb_table = duckdb_table + "\n".join(duckdb_scores.prev_scores)
    sqlite_table = sqlite_table + "\n".join(sqlite_scores.prev_scores)


    incremental_md_table = (
    f"""# Results

    ## Timer (Incremental Updates)
    | Method | Time (milliseconds) |
    | ------ | -------------- |
    | duckdb | {mean(duckdb_update_time) * 1000:.2f} |
    | sqlite | {mean(sqlite_update_time) * 1000:.2f} |
    """)

    batch_md_table = (
    f"""## Timer (Batch Updates)
    | Method | Time (seconds) |
    | ------ | -------------- |
    | duckdb | {mean(duckdb_batch_update_time):.2f} |
    | sqlite | {mean(sqlite_batch_update_time):.2f} |
    """)

    result_file = open("./results/nfl_pbp_updates.md", 'w+')
    result_file.write(incremental_md_table + batch_md_table + duckdb_table + sqlite_table)
    result_file.close()

import timeit
from dataclasses import dataclass
from typing import Iterator

import duckdb
import sqlite3
import random

@dataclass
class Scores:
    home_score: int
    away_score: int
    index: int

    def __init__(self):
        self.home_score = random.randrange(100)
        self.away_score = random.randrange(100)
        self.index = random.randrange(1230855)

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


@dataclass
class DuckDbScores(Scores):
    def __init__(self):
        super().__init__()

@dataclass
class SqliteScores(Scores):
    def __init__(self):
        super().__init__()


@dataclass
class ScoreHolder:
    duckdb_scores = DuckDbScores()
    duckdb_score_list = []
    sqlite_scores = SqliteScores()
    sqlite_score_list = []

    def reset_duckdb_scores(self):
        self.duckdb_score_list.append(self.duckdb_scores)
        self.duckdb_scores = DuckDbScores()

    def reset_sqlite_scores(self):
        self.sqlite_score_list.append(self.sqlite_scores)
        self.sqlite_scores = SqliteScores()

    def __iter__(self) -> Iterator[tuple[DuckDbScores, SqliteScores]]:
        return iter(zip(self.duckdb_score_list, self.sqlite_score_list))


duckdb_conn = duckdb.connect("dbs/nfl_pbp.duckdb")
sqlite_conn = sqlite3.connect("dbs/nfl_pbp.db")

score_holder = ScoreHolder()

def duckdb_update(scores: Scores):
    duckdb_conn.execute(f"UPDATE pbp "
                        f"SET "
                        f"total_home_score = {scores.home_score}, "
                        f"total_away_score = {scores.away_score}, "
                        f"result = {scores.result}, "
                        f"total = {scores.total} "
                        f"WHERE idx = {scores.index}")


def duckdb_batch_update():
    duckdb_conn.execute("UPDATE pbp "
                        "SET "
                        "total_home_score = 10, "
                        "total_away_score = 20, "
                        "result = -10, "
                        "total = 30 "
                        "WHERE home_team = 'NE'")


def sqlite_update(scores: Scores):
    sqlite_conn.execute(f"UPDATE pbp "
                        f"SET "
                        f"total_home_score = {scores.home_score}, "
                        f"total_away_score = {scores.away_score}, "
                        f"result = {scores.result}, "
                        f"total = {scores.total} "
                        f"WHERE idx = {scores.index}")


def sqlite_batch_update():
    sqlite_conn.execute("UPDATE pbp "
                        "SET "
                        "total_home_score = 10, "
                        "total_away_score = 20, "
                        "result = -10, "
                        "total = 30 "
                        "WHERE home_team = 'NE'")

print("Processing duckdb incremental update")
duckdb_update_time = timeit.repeat(
    stmt=lambda : duckdb_update(score_holder.duckdb_scores),
    setup=score_holder.reset_duckdb_scores,
    repeat=10000,
    number=1
)

print("Processing sqlite incremental update")
sqlite_update_time = timeit.repeat(
    stmt=lambda : sqlite_update(score_holder.sqlite_scores),
    setup=score_holder.reset_sqlite_scores,
    repeat=10000,
    number=1
)
print("Done")

print(f"duckdb_update took on avg {(sum(duckdb_update_time)/10000) * 1000:.2f} milliseconds")
print(f"sqlite_update took on avg {(sum(sqlite_update_time)/10000) * 1000:.2f} milliseconds")


print("Processing duckdb batch update")
duckdb_batch_update_time = timeit.timeit(
    stmt=duckdb_batch_update,
    number=100
)

print("Processing sqlite batch update")
sqlite_batch_update_time = timeit.timeit(
    stmt=sqlite_batch_update,
    number=100
)
print("Done")

print(f"duckdb_batch_update took on avg {duckdb_batch_update_time:.2f} seconds")
print(f"sqlite_batch_update took on avg {sqlite_batch_update_time:.2f} seconds")

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

for duckdb_score, sqlite_score in score_holder:
    duckdb_table = duckdb_table + duckdb_score.to_md() + "\n"
    sqlite_table = sqlite_table + sqlite_score.to_md() + "\n"


incremental_md_table = (
f"""# Results

## Timer (Incremental Updates)
| Method | Time (milliseconds) |
| ------ | -------------- |
| duckdb | {(sum(duckdb_update_time)/10000) * 1000:.2f} |
| sqlite | {(sum(sqlite_update_time)/10000) * 1000:.2f} |
""")

batch_md_table = (
f"""## Timer (Batch Updates)
| Method | Time (seconds) |
| ------ | -------------- |
| duckdb | {duckdb_batch_update_time:.2f} |
| sqlite | {sqlite_batch_update_time:.2f} |
""")

result_file = open("./results/nfl_pbp_updates.md", 'w+')
result_file.write(incremental_md_table + batch_md_table + duckdb_table + sqlite_table)
result_file.close()

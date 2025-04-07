# Duckdb Sqlite Comparison

This repo contains comparison scripts showcasing the performance difference between duckdb and sqlite.
Uses the NFL Play-By-Play datasets found [https://github.com/nflverse/nflverse-data/releases/tag/pbp](https://github.com/nflverse/nflverse-data/releases/tag/pbp)

To use this repo, download the parquet files from the link above into the [parquet_files](./parquet_files) directory.  Then
run the scripts in this order:

1. [nfl_pbp_to_db.py](./nfl_pbp_to_db.py)
1. [nfl_pbp_aggs.py](./nfl_pbp_aggs.py)
1. [nfl_pbp_updates.py](./nfl_pbp_updates.py)


## nfl_pbp_to_db

This script will load the parquet data into duckdb and sqlite and time how long it takes
to create and load the tables


## nfl_pbp_aggs

This script will run a sum query and time how long it takes execute

## nfl_pbp_updates

This script will run 2 update queries:

1. First, incremental updates to multiple rows with new values
2. Second batch updates to multiple rows based on a single where condition (`WHERE home_team = 'NE'`)


These scripts then write the results to markdown files

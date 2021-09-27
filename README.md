# DuckDB driver for LDBC Graphalytics

Platform driver for the [LDBC Graphalytics benchmark](https://graphalytics.org) with an embedded [DuckDB](https://duckdb.org/) instance.

To execute the Graphalytics benchmark on DuckDB, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the DuckDB-specific instructions listed below.

# Get DuckDB

Run:
```
bin/sh/get-duckdb.sh
```

To test, adjust the test.sh script. :warning: it will amend your last commit!
```
./test.sh
```

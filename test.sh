pushd . && git ci -a --amend --no-edit && ./init.sh && cd graphalytics-1.4.0-duckdb-0.1-SNAPSHOT/ && bin/sh/run-benchmark.sh ; popd && cat /tmp/lcc.csv

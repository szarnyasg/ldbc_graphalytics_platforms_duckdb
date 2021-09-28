#!/bin/bash

set -e

if [ "$(uname)" == "Darwin" ]; then
  rootdir=$(dirname $(greadlink -f ${BASH_SOURCE[0]}))/../..
else
  rootdir=$(dirname $(readlink -f ${BASH_SOURCE[0]}))/../..
fi

# Parse commandline instructions (provided by Graphalytics).
while [[ $# -gt 1 ]] # Parse two arguments: [--key value] or [-k value]
  do
  key="$1"
  value="$2"

  case ${key} in

    --graph-name)
      # not used
      shift;;

    --input-vertex-path)
      INPUT_VERTEX_PATH="$value"
      shift;;

    --input-edge-path)
      INPUT_EDGE_PATH="$value"
      shift;;

    --output-path)
      OUTPUT_PATH="$value"
      shift;;

    --directed)
      DIRECTED="$value"
      shift;;

    --weighted)
      WEIGHTED="$value"
      shift;;

    *)
      echo "Error: invalid option: " "$key"
      exit 1
      ;;
  esac
  shift
done

DB_PATH=/tmp/gx.duckdb

rm -f ${DB_PATH} ${DB_PATH}.wal

if [ ${WEIGHTED} == "true" ]; then
  LOAD_WEIGHT_ATTRIBUTE=", weight"
  CREATE_WEIGHT_ATTRIBUTE=", weight FLOAT"
else
  LOAD_WEIGHT_ATTRIBUTE=""
  CREATE_WEIGHT_ATTRIBUTE=""
fi

bin/duckdb ${DB_PATH} "CREATE TABLE v(id INTEGER); "
bin/duckdb ${DB_PATH} "COPY v (id) FROM '${INPUT_VERTEX_PATH}' (DELIMITER ' ', FORMAT csv); "
bin/duckdb ${DB_PATH} "CREATE TABLE e(source INTEGER, target INTEGER${CREATE_WEIGHT_ATTRIBUTE}); " 
bin/duckdb ${DB_PATH} "COPY e (source, target${LOAD_WEIGHT_ATTRIBUTE}) FROM '${INPUT_EDGE_PATH}' (DELIMITER ' ', FORMAT csv); "
if [ ${DIRECTED} == "false" ]; then
  bin/duckdb ${DB_PATH} "COPY e (target, source${LOAD_WEIGHT_ATTRIBUTE}) FROM '${INPUT_EDGE_PATH}' (DELIMITER ' ', FORMAT csv); "
fi

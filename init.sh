#/bin/sh

set -e

GRAPHS_DIR=${1:-~/graphs}
MATRICES_DIR=${2:-~/matrices}
GRAPHALYTICS_VERSION=1.4.0
PROJECT_VERSION=0.1-SNAPSHOT
PROJECT=graphalytics-$GRAPHALYTICS_VERSION-duckdb-$PROJECT_VERSION

rm -rf $PROJECT
mvn package
tar xf $PROJECT-bin.tar.gz
cd $PROJECT/

cp -r config-template config
# set directories
sed -i.bkp "s|^graphs.root-directory =$|graphs.root-directory = $GRAPHS_DIR|g" config/benchmark.properties
sed -i.bkp "s|^graphs.validation-directory =$|graphs.validation-directory = $GRAPHS_DIR|g" config/benchmark.properties
# set the number of threads to use
sed -i.bkp "s|^platform.duckdb.num-threads =$|platform.duckdb.num-threads = $(nproc --all)|g" config/platform.properties

bin/sh/compile-benchmark.sh

if [ -d $MATRICES_DIR ]; then
	bin/sh/link-matrix-market-graphs.sh $MATRICES_DIR
fi

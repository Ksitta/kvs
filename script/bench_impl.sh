#!/bin/bash
set -e
set -o xtrace

DIR="./tmp_bench"

mkdir -p ../build
cd ../build || exit 1
cmake -DCMAKE_BUILD_TYPE=Release ..; make -j

rm -rf "${DIR:?}/"
mkdir -p "${DIR}"

bash -c "./bench/bench --kvdir ${DIR} $@"

cd ../bench || exit 1

#!/bin/bash
set -e
set -o xtrace

DIR="./tmp_test"

mkdir -p ../build
cd ../build || exit 1
cmake -DCMAKE_BUILD_TYPE=Debug ..; make -j

rm -rf "${DIR:?}/"
mkdir -p "${DIR}"

cd test
arr=($1*)
cd ..
for test in "${arr[@]}"; do
    echo "Running ${test}"
    bash -c "./test/${test} --kvdir ${DIR}"
    rm -rf "${DIR:?}/"
    mkdir -p "${DIR}"
done

cd ../script || exit 1



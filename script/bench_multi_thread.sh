#!/bin/bash
set -e
set -o xtrace

./bench_impl.sh --thread_nr=8

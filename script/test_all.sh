#!/bin/bash
set -e
set -o xtrace

./test_impl.sh basic
./test_impl.sh crash
./test_impl.sh multithread
./test_impl.sh bonus

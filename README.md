# SSK-based Key Value Store

## All you have to do

Complete `src/engine.cpp` to pass all the tests.

Try to optimize your implementation for higher performance.

The API is defined in `include/engine.h`

## Prerequisite & Building

The project depends on [glog](https://github.com/google/glog) and [gflags](https://github.com/gflags/gflags).

If you are on Ubuntu, you can set up the dependency by

``` bash
sudo ./bootstrap.sh
```

To build the project

```
# build it in Debug mode
cmake -DCMAKE_BUILD_TYPE=Debug ..
# or build it in Release mode for benchmarking
# cmake -DCMAKE_BUILD_TYPE=Release
make -j
```

## Correctness Test

The tests are placed in `test/` directory.

The correctness tests are categorized into several groups.
- The `test/basic_*` checks the basic correctness of your KV.
- The `test/multithread_*` checks whether your KV works well with multiple threads.
- The `test/crash_*` checks the crash consistency of your KV.
- The `test/bonus_*` is the optional features you may implement for bonus.

There are convenients scripts for you to tests your KV.

``` bash
cd scripts
./test_basic.sh
./test_multithread.sh
./test_crash.sh
./test_bonus.sh

# all you want to test them all
./test_all.sh
```
## Performance Test

``` bash
cd script
./bench_single_thread.sh
./bench_multi_thread.sh
```

# NOTICE

### 1. About key/value size

The key size will be no larger than `kvs::kMaxKeySize`, while the value `kvs::kMaxValueSize`.

See `include/conf.h` for the definitions.

# More Details (If you are interested)

## Tuning benchmark

The benchmark defines a lot of tunable parameters.

For example, `bench/bench.cpp` defines

``` c++
DEFINE_uint32(read_ratio, 80, "[0-100] the percentage of read");
```

Then, you can pass in `--read_ratio=50` to modify the default value (which is 80) for setting the read operation ratio.

i.e.

``` bash
cd script
./bench_single_thread.sh --read_ratio=20
./bench_multipe_thread.sh --read_ratio=20
```

## Formating the project

Install `clang-format` to format your code.

The project already provide you a `.clang-format` to specify the foramt.

## Debugging

You can uncomment the below lines to enable [ASAN](https://github.com/google/sanitizers/wiki/AddressSanitizer).

It is a convenient tool for addressing memory problems.

``` cmake
# set(ASAN_OPTIONS "fast_unwind_on_fatal=1")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")

```
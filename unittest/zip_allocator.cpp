#include <iostream>

#include "util/rand_alloc.h"
using namespace bench;

constexpr static size_t kTestTime = 1000;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    bench::ZipfianAllocator alloc(100, 1000, 0.99);
    for (size_t i = 0; i < kTestTime; ++i)
    {
        auto k = alloc.alloc_key(sizeof(uint64_t));
        LOG(INFO) << *(uint64_t *) k.data();
    }

    return 0;
}
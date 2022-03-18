#include <iostream>

#include "util/bench.h"
using namespace bench;

constexpr static ssize_t kTestTime = 1ull * 1000 * 1000;
constexpr static ssize_t kLogNr = 100;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    for (ssize_t i = 0; i < kTestTime; ++i)
    {
        auto db = fast_pseudo_rand_dbl(0, 5);
        CHECK_GE(db, 0);
        CHECK_LT(db, 5);
        if (i % (kTestTime / kLogNr) == 0)
        {
            LOG(INFO) << db;
        }
    }

    size_t total_nr = 0;
    size_t yes_nr = 0;

    for (ssize_t i = 0; i < kTestTime; ++i)
    {
        if (probability_yes(0.2))
        {
            yes_nr++;
        }
        total_nr++;
    }

    double rate = 1.0 * yes_nr / total_nr;
    CHECK_LT(abs(rate - 0.2), 0.01)
        << "The error of rate should be less than 1%%";

    LOG(INFO) << "PASS";

    return 0;
}
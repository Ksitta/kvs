#include <array>
#include <iostream>

#include "util/bench.h"
using namespace bench;

constexpr static ssize_t kTestTime = 1ull * 1000 * 1000;
constexpr static ssize_t kLogNr = 100;

void test_prob_yes()
{
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
}

void test_dbl()
{
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
}

void test_random_choose()
{
    constexpr static size_t kItemNr = 100;
    std::set<int> s;
    for (size_t i = 0; i < kItemNr; ++i)
    {
        s.insert(i);
    }
    std::array<size_t, kItemNr> bucket;
    bucket.fill(0);
    for (size_t i = 0; i < kTestTime; ++i)
    {
        int val = random_choose(s);
        CHECK_GE(val, 0);
        CHECK_LT(val, kItemNr);
        bucket[val]++;
        CHECK_LE(bucket[val], kTestTime);
    }
    for (size_t i = 0; i < bucket.size(); ++i)
    {
        double rate = 1.0 * bucket[i] / kTestTime;
        double expect_rate = 1.0 / kItemNr;
        CHECK_LT(abs(rate - expect_rate), 0.01)
            << "Expect the probability error < 1%%. for i-th: " << i
            << ", bucket[" << i << "] = " << bucket[i] << ". rate: " << rate
            << ", expect_rate: " << expect_rate;
    }
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;
    test_dbl();

    test_prob_yes();

    test_random_choose();

    LOG(INFO) << "PASS";

    return 0;
}
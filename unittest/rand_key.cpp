#include <iostream>

#include "util/bench.h"
using namespace bench;

constexpr static ssize_t kTestTime = 1ull * 1000 * 1000;
constexpr static ssize_t kLogNr = 100;
constexpr static size_t kKeySize = 100;
constexpr static size_t kValueSize = 100;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    bool key_have_upper_bound = false;
    bool value_have_upper_bound = false;
    for (ssize_t i = 0; i < kTestTime; ++i)
    {
        auto key_size = fast_pseudo_rand_int(0, kKeySize);
        auto value_size = fast_pseudo_rand_int(0, kValueSize);
        CHECK_GE(key_size, 0);
        CHECK_GE(value_size, 0);
        CHECK_LE(key_size, kKeySize);
        CHECK_LE(value_size, kValueSize);
        if (key_size == kKeySize)
        {
            key_have_upper_bound = true;
        }
        if (value_size == kValueSize)
        {
            value_have_upper_bound = true;
        }
        auto key = gen_rand_key(key_size);
        auto value = gen_rand_value(value_size);
        CHECK_EQ(key.size(), key_size);
        CHECK_EQ(value.size(), value_size);
        for (size_t i = 0; i < key.size(); ++i)
        {
            CHECK(isalnum(key[i]));
        }
        for (size_t i = 0; i < value.size(); ++i)
        {
            CHECK(isalnum(value[i]));
        }
        if (i % (kTestTime / kLogNr) == 0)
        {
            LOG(INFO) << "Sampled. key: `" << key << "`, value: `" << value
                      << "`";
        }
    }
    CHECK(key_have_upper_bound);
    CHECK(value_have_upper_bound);
    LOG(INFO) << "PASS";

    return 0;
}
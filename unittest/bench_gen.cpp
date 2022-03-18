#include <iostream>

#include "util/bench.h"
using namespace bench;

constexpr static size_t kTestTime = 100000;

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    auto allocator = std::make_shared<PrintableAllocator>();
    bench::BenchGenerator g({{Op::kPut, 0.4, 32, 4, 0.2, 0},
                             {Op::kGet, 0.4, 32, 4, 0.4, 0},
                             {Op::kDelete, 0.1, 32, 4, 0.6, 0},
                             {Op::kVisit, 0.1, 0, 0, 0, 0.8}},
                            allocator);

    std::map<Key, Value> kv;
    auto test = g.rand_tests(kTestTime, 0);

    size_t get_found = 0;
    size_t get_nr = 0;
    size_t delete_found = 0;
    size_t del_nr = 0;
    size_t insert_found = 0;
    size_t ins_nr = 0;
    double range_length = 0;
    size_t rng_nr = 0;
    for (const auto &t : test)
    {
        if (t.op == Op::kGet)
        {
            if (kv.count(t.key) == 1)
            {
                get_found++;
            }
            get_nr++;
        }
        else if (t.op == Op::kPut)
        {
            if (kv.count(t.key) == 1)
            {
                insert_found++;
            }
            ins_nr++;
            kv[t.key] = t.value;
        }
        else if (t.op == Op::kDelete)
        {
            if (kv.count(t.key) == 1)
            {
                delete_found++;
            }
            del_nr++;
            kv.erase(t.key);
        }
        else if (t.op == Op::kVisit)
        {
            if (kv.empty())
            {
                continue;
            }
            auto lower = kv.lower_bound(t.lower);
            auto upper = kv.upper_bound(t.upper);
            auto diff = std::distance(lower, upper);
            range_length += 1.0 * diff / kv.size();
            rng_nr++;
            // LOG(INFO) << "len: " << range_length << ", diff: " << diff
            //           << ", kv_size: " << kv.size();
        }
        else if (t.op == Op::kNoOp)
        {
        }
        else
        {
            CHECK(false) << "unknown op " << t.op;
        }
    }
    LOG(INFO) << "get_found_rate: " << 1.0 * get_found / get_nr
              << ", ins_found_rate: " << 1.0 * insert_found / ins_nr
              << ", del_found_rate: " << 1.0 * delete_found / del_nr
              << ", visit_rng: " << 1.0 * range_length / rng_nr;
    CHECK_LT(std::abs(1.0 * get_found / get_nr - 0.4), 0.01);
    CHECK_LT(std::abs(1.0 * insert_found / ins_nr - 0.2), 0.01);
    CHECK_LT(std::abs(1.0 * delete_found / del_nr - 0.6), 0.01);
    CHECK_LT(std::abs(1.0 * range_length / rng_nr - 0.8), 0.01);

    return 0;
}
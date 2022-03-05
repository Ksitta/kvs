#include <iostream>

#include "engine.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 20000, "The number of tests");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");
DEFINE_uint32(thread_nr, 8, "The number of threads to run");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    LOG(INFO) << "constructing Engine...";

    auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});

    std::vector<std::thread> threads;
    std::vector<std::vector<TestCase>> test_cases;

    LOG(INFO) << "Generating data...";

    std::vector<std::string> key_set({"a", "b", "c"});
    std::vector<std::string> value_set({"one", "two", "three"});

    std::vector<std::shared_ptr<IRandAllocator>> allocators;
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        allocators.emplace_back(
            std::make_shared<SetAllocator>(key_set, value_set));
    }
    std::vector<bench::BenchGenerator> bench_generators;
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        bench::BenchGenerator g({{Op::kGet, 0.15, 2, 4, 0.2, 0},
                                 {Op::kPut, 0.7, 2, 4, 0.3, 0},
                                 {Op::kDelete, 0.15, 2, 4, 0.3, 0}},
                                allocators[i]);
        bench_generators.emplace_back(std::move(g));
    }
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        test_cases.emplace_back(
            bench_generators[i].rand_tests(FLAGS_test_nr, 0));
    }

    LOG(INFO) << "Starting tests...";
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        threads.emplace_back([engine, &test_case = test_cases[i]]() {
            EngineValidator executor(engine);
            executor.execute(test_case);
        });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    size_t got = 0;
    engine->visit(
        "", "", [&key_set, &value_set, &got](const Key &k, const Value &v) {
            auto it = std::find(key_set.begin(), key_set.end(), k);
            if (it == key_set.end())
            {
                LOG(FATAL) << "Key " << k << " not in key_set";
            }
            auto it2 = std::find(value_set.begin(), value_set.end(), v);
            if (it2 == value_set.end())
            {
                LOG(FATAL) << "Value " << v << " not in value_set";
            }
            got++;
        });
    CHECK_GE(got, 0);
    CHECK_LE(got, key_set.size());

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
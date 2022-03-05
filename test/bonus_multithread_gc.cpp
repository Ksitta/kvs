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
    auto allocator = std::make_shared<PrintableAllocator>();
    bench::BenchGenerator g({{Op::kGet, 0.15, 2, 4, 0.2, 0},
                             {Op::kPut, 0.7, 2, 4, 0.3, 0},
                             {Op::kDelete, 0.14, 2, 4, 0.3, 0},
                             {Op::kGc, 0.01, 0, 0, 0, 0}},
                            allocator);
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        test_cases.emplace_back(g.rand_tests(FLAGS_test_nr, 0));
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

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
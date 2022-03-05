#include <chrono>
#include <iostream>
#include <thread>

#include "conf.h"
#include "engine.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;
using namespace std::chrono_literals;

DEFINE_uint32(thread_nr, 8, "The number of threads to run");
DEFINE_uint32(snapshot_thread_nr, 8, "The number of threads to query snapshot");
DEFINE_uint64(test_nr, 20000, "The number of tests");
DEFINE_uint64(ro_test_nr, 1000, "The number of read only tests");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

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
    std::vector<std::vector<TestCase>> ro_test_cases;

    LOG(INFO) << "Generating data...";
    auto allocator = std::make_shared<PrintableAllocator>();
    bench::BenchGenerator g({{Op::kGet, 0.15, 16, 4, 0.2, 0},
                             {Op::kPut, 0.7, 16, 4, 0.3, 0},
                             {Op::kDelete, 0.15, 16, 4, 0.3, 0}},
                            allocator);
    auto snapshot_g = g.clone(
        {{Op::kGet, 0.5, 16, 4, 0.2, 0}, {Op::kVisit, 0.5, 0, 0, 0, 0.5}});

    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        test_cases.emplace_back(g.rand_tests(FLAGS_test_nr, 0));
    }
    for (size_t i = 0; i < FLAGS_snapshot_thread_nr; ++i)
    {
        ro_test_cases.emplace_back(snapshot_g.rand_tests(FLAGS_ro_test_nr, 0));
    }

    LOG(INFO) << "Starting tests...";
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        threads.emplace_back([engine, &test_case = test_cases[i]]() {
            EngineValidator executor(engine);
            executor.execute(test_case);
        });
    }

    std::this_thread::sleep_for(10ms);

    auto snapshot = CHECK_NOTNULL(engine->snapshot());

    std::vector<std::thread> snapshot_threads;

    for (size_t i = 0; i < FLAGS_snapshot_thread_nr; ++i)
    {
        snapshot_threads.emplace_back(
            [engine, &ro_test_case = ro_test_cases[i]]() {
                EngineValidator executor(engine);
                executor.execute(ro_test_case);
            });
    }

    for (auto &t : threads)
    {
        t.join();
    }
    for (auto &t : snapshot_threads)
    {
        t.join();
    }

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
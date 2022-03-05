#include "util/bench.h"

#include <atomic>
#include <iostream>
#include <thread>

#include "conf.h"
#include "engine.h"
#include "glog/logging.h"
#include "util/executor.h"
#include "util/rand_alloc.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 100000, "The number of tests");
DEFINE_string(kvdir, kDefaultBenchDir, "The KV Store data directory");
DEFINE_uint64(max_key, 10 * 1000, "The max key");
DEFINE_uint64(max_value_size, 4 * 1024, "The max value size");
DEFINE_uint64(thread_nr, 8, "The number of threads");
DEFINE_uint32(read_ratio, 80, "[0-100] the percentage of read");
DEFINE_bool(is_skew, true, "Whether uniform or zipfian distribution");
DEFINE_double(zip_para, 0.99, "The zipfian distribution parameter");
DEFINE_uint64(rand_seed, 2022, "The seed to run");
DEFINE_uint64(sync_every, 10, "How many operations between each sync op");
DEFINE_int64(
    execute_batch,
    100,
    "Execute FLAGS_execute_batch before synchronize to the std::atomic");
DEFINE_double(overwrite_ratio,
              0.2,
              "[0, 1] How may operation may hit to an existing keys");
void explain()
{
    LOG(INFO) << "[explain] test_nr: " << FLAGS_test_nr
              << ", max_key: " << FLAGS_max_key
              << ", max_value_size: " << FLAGS_max_value_size
              << ", thread_nr: " << FLAGS_thread_nr
              << ", read_ratio[0-100]: " << FLAGS_read_ratio
              << ", is_skew: " << FLAGS_is_skew
              << ", zip_para: " << FLAGS_zip_para
              << ", sync_every: " << FLAGS_sync_every
              << ", overwrite_ratio: " << FLAGS_overwrite_ratio;
}

IEngine::Key to_key(uint64_t k)
{
    return IEngine::Key((char *) &k, sizeof(uint64_t));
}

void warmup(IEngine::Pointer engine)
{
    std::string value(FLAGS_max_value_size / 2, 'a');

    for (size_t k = 0; k < FLAGS_max_key; ++k)
    {
        auto key = to_key(k);
        CHECK_EQ(engine->put(key, value), kSucc);
    }
}

void bench_kv(IEngine::Pointer engine)
{
    std::vector<std::thread> threads;

    std::atomic<ssize_t> task_nr{(ssize_t) FLAGS_test_nr};

    std::vector<std::shared_ptr<bench::IRandAllocator>> allocators;
    std::vector<std::vector<bench::TestCase>> test_cases;

    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        if (FLAGS_is_skew)
        {
            allocators.emplace_back(std::make_shared<bench::ZipfianAllocator>(
                FLAGS_rand_seed, FLAGS_max_key, FLAGS_zip_para));
        }
        else
        {
            allocators.emplace_back(
                std::make_shared<bench::UniformAllocator>(FLAGS_max_key));

            LOG_IF(WARNING, FLAGS_zip_para != 0.99)
                << "When --is_skew=false, we ignore --zip_para";
        }
        double read_ratio = 1.0 * FLAGS_read_ratio / 100;
        double ins_ratio = (1 - read_ratio) / 2;
        double del_ratio = ins_ratio;
        bench::BenchGenerator g({{Op::kGet,
                                  read_ratio,
                                  sizeof(uint64_t),
                                  FLAGS_max_value_size,
                                  FLAGS_overwrite_ratio,
                                  0},
                                 {Op::kPut,
                                  ins_ratio,
                                  sizeof(uint64_t),
                                  FLAGS_max_value_size,
                                  FLAGS_overwrite_ratio,
                                  0},
                                 {Op::kDelete,
                                  del_ratio,
                                  sizeof(uint64_t),
                                  FLAGS_max_value_size,
                                  FLAGS_overwrite_ratio,
                                  0}},
                                allocators.back());
        test_cases.emplace_back(g.rand_tests(FLAGS_test_nr, FLAGS_sync_every));
    }

    LOG(INFO) << "Start benchmarking...";
    auto now = std::chrono::steady_clock::now();
    for (size_t i = 0; i < FLAGS_thread_nr; ++i)
    {
        threads.emplace_back([&task_nr, &test_case = test_cases[i], engine]() {
            EngineValidator executor(engine);

            auto remain_task_nr = task_nr.load(std::memory_order_relaxed);
            size_t cur_idx = 0;
            while (remain_task_nr > 0)
            {
                auto current_task_nr =
                    std::min(remain_task_nr, FLAGS_execute_batch);

                executor.execute(test_case, cur_idx, current_task_nr);

                task_nr.fetch_sub(current_task_nr, std::memory_order_relaxed);
                remain_task_nr = task_nr.load(std::memory_order_relaxed);
                cur_idx += current_task_nr;
            }
        });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();

    auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - now).count();
    LOG(INFO) << "[bench] executing " << FLAGS_test_nr << " tests with "
              << FLAGS_thread_nr << " threads, take " << ns << " ns";
    double ops = 1e9 * FLAGS_test_nr / ns;
    double ops_thread = ops / FLAGS_thread_nr;
    double avg_lat = ns / FLAGS_test_nr;
    LOG(INFO) << "[summary] ops: " << ops << ", ops(thread): " << ops_thread
              << ", avg_lat: " << avg_lat << " ns";
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    CHECK_GE(FLAGS_read_ratio, 0);
    CHECK_LE(FLAGS_read_ratio, 100);
    CHECK_NE(FLAGS_kvdir, "");
    CHECK_GE(FLAGS_zip_para, 0.0);
    CHECK_LE(FLAGS_zip_para, 2.0) << "Never know this can be >= 2";
    CHECK_GE(FLAGS_overwrite_ratio, 0.0);
    CHECK_LE(FLAGS_overwrite_ratio, 1.0);

    explain();

    LOG(INFO) << "constructing Engine...";

    auto e = Engine::new_instance(FLAGS_kvdir, EngineOptions{});

    warmup(e);

    bench_kv(e);

    return 0;
}
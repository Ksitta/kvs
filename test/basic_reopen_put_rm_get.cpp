#include <iostream>

#include "engine.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 1000, "The number of tests");
DEFINE_uint64(reopen_nr, 100, "The number of engine reopen");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    auto allocator = std::make_shared<PrintableAllocator>();
    bench::BenchGenerator g({{Op::kGet, 0.15, 2, 4, 0.2, 0},
                             {Op::kPut, 0.7, 2, 4, 0.3, 0},
                             {Op::kDelete, 0.15, 2, 4, 0.3, 0}},
                            allocator);

    std::map<Key, Value> kv;
    for (size_t i = 0; i < FLAGS_reopen_nr; ++i)
    {
        auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});
        EngineValidator executor(engine, kv);
        executor.validate_execute(g.rand_tests(FLAGS_test_nr, 0));
        kv = executor.get_internal_kv();
    }

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
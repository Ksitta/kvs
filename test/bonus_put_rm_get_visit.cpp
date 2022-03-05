#include <iostream>

#include "engine.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 100000, "the number of tests");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    auto allocator = std::make_shared<PrintableAllocator>();
    bench::BenchGenerator g({{Op::kGet, 0.20, 2, 4, 0.2, 0},
                             {Op::kPut, 0.5, 2, 4, 0.3, 0},
                             {Op::kDelete, 0.20, 2, 4, 0.3, 0},
                             {Op::kVisit, 0.1, 0, 0, 0, 0.5}},
                            allocator);

    LOG(INFO) << "constructing Engine...";
    auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});

    EngineValidator executor(engine);

    LOG(INFO) << "validating...";
    auto rw_test = g.rand_tests(FLAGS_test_nr, 0);

    executor.validate_execute(rw_test);

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
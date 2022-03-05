#include <iostream>

#include "conf.h"
#include "engine.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 10000, "The number of tests");
DEFINE_uint64(ro_test_nr, 5000, "The number of tests");
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

    auto snapshot_g = g.clone(
        {{Op::kGet, 0.5, 16, 4, 0.2, 0}, {Op::kVisit, 0.5, 0, 0, 0, 0.5}});
    auto ro_test = snapshot_g.rand_tests(FLAGS_ro_test_nr, 0);

    executor.validate_execute(rw_test);

    auto snapshot_validator = executor.snapshot_validator();

    // run concurrent query on the snapshot
    std::thread snapshot_thread([&snapshot_validator, &ro_test]() {
        snapshot_validator.validate_execute(ro_test);
    });

    // run concurrent insert to the original db
    executor.validate_execute(g.rand_tests(FLAGS_test_nr, 0));

    snapshot_thread.join();

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
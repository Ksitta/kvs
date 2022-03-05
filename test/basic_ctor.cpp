#include <iostream>

#include "engine.h"
#include "glog/logging.h"

using namespace kvs;

DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    auto e = kvs::Engine::new_instance(FLAGS_kvdir, EngineOptions{});

    LOG(INFO) << "Succeeded in constructing engine!";

    return 0;
}
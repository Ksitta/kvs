#include <iostream>

#include "engine.h"
#include "glog/logging.h"

using namespace kvs;

DEFINE_uint64(test_nr, 100000, "The number of tests");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    LOG(INFO) << "constructing Engine...";

    auto e = Engine::new_instance(FLAGS_kvdir, EngineOptions{});

    CHECK_EQ(e->put("a", "abc"), kSucc);

    std::string value;
    CHECK_EQ(e->get("a", value), kSucc);
    CHECK_EQ("abc", value);

    CHECK_EQ(e->remove("a"), kSucc);

    std::string backup_value = value;
    CHECK_EQ(e->get("a", value), kNotFound);
    CHECK_EQ(value, backup_value)
        << "A failed op should not modify the @value field";

    LOG(INFO) << "PASS " << argv[0];

    return 0;
}
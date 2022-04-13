#include <iostream>

#include "engine.h"
#include "glog/logging.h"
#include "util/bench.h"
#include "util/executor.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(test_nr, 1000000, "The number of tests");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

int main(int argc, char *argv[])
{

    auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});


    std::string keys[100];
    for(int i = 0; i < 100; i++){
        keys[i] = gen_rand_key(10);
    }
    for(int i = 0; i < 1000; i++){
        std::string val = gen_rand_value(100000);
        engine->put(keys[i % 100], val);
        engine->remove(keys[(i + 5) % 100]);
    }
    char a;
    std::cout << "Check File Size and Input Anything\n";
    std::cin >> a;
    std::cout << "Check File Size Again\n";
    engine->garbage_collect();

    return 0;
}
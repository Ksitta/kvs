#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <string>

#include "conf.h"
#include "engine.h"
#include "test_util.h"
#include "util/bench.h"
#include "util/executor.h"
#include "util/rand_alloc.h"

using namespace kvs;
using namespace bench;

DEFINE_uint64(kill_at, 1000, "After how many tests to signal kill");
DEFINE_string(kvdir, kDefaultTestDir, "The KV Store data directory");

std::atomic<bool> parent_can_kill{false};
void handler(int signal)
{
    (void) signal;
    parent_can_kill = true;
}

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    FLAGS_colorlogtostderr = true;

    std::string value(8 * 1024, 'a');
    std::string value2(8 * 1024, 'b');

    CHECK_EQ(FLAGS_kill_at % 10, 0)
        << "Let kill_at be multiple of 10, which is the sync interval";

    signal(SIGUSR1, handler);
    pid_t fpid = fork();
    if (fpid == 0)
    {
        // child
        auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});
        CHECK_EQ(engine->put("a", "abc"), kSucc);
        CHECK_EQ(engine->put("b", "kkk"), kSucc);
        CHECK_EQ(engine->put("c", "ooo"), kSucc);
        CHECK_EQ(engine->put("d", "jjjsdfsdf"), kSucc);
        CHECK_EQ(engine->put("e", "asdfsdgs"), kSucc);
        CHECK_EQ(engine->put("a", "def"), kSucc);
        CHECK_EQ(engine->remove("b"), kSucc);
        CHECK_EQ(engine->remove("c"), kSucc);
        kill(getppid(), SIGUSR1);
        while (true)
        {
        }
    }
    else if (fpid > 0)
    {  // me

        while (!parent_can_kill)
        {
        }

        LOG(INFO) << "Recv signal. parant can kill";

        int res = kill(fpid, 9);
        CHECK_EQ(res, 0);

        waitpid(fpid, &res, 0);
        CHECK_GT(res, 0);

        // re-open and check
        auto engine = Engine::new_instance(FLAGS_kvdir, EngineOptions{});

        std::string value;
        CHECK_EQ(engine->get("a", value), kSucc);
        CHECK_EQ("def", value);
        CHECK_EQ(engine->get("b", value), kNotFound);
        CHECK_EQ(engine->get("c", value), kNotFound);
        CHECK_EQ(engine->get("d", value), kSucc);
        CHECK_EQ("jjjsdfsdf", value);
        CHECK_EQ(engine->get("e", value), kSucc);
        CHECK_EQ("asdfsdgs", value);
        CHECK_EQ(engine->get("f", value), kNotFound);
        CHECK_EQ(engine->get("g", value), kNotFound);
    }
    else
    {
        assert(false);
    }

    LOG(INFO) << "PASS " << argv[0];

    return 0;
};

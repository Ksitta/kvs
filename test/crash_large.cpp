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
#include "util/bench.h"
#include "util/executor.h"
#include "util/rand_alloc.h"
#include "util/utils.h"

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
        size_t cnt = 0;
        while (true)
        {
            // do db task here

            auto key = std::to_string(cnt);
            CHECK_EQ(engine->put(key, value), kSucc);
            if (cnt % 4 == 1)
            {
                CHECK_EQ(engine->remove(key), kSucc);
            }
            if (cnt % 4 == 2)
            {
                CHECK_EQ(engine->put(key, value2), kSucc);
            }

            if (cnt % 10 == 0)
            {
                engine->sync();
            }

            if (cnt == FLAGS_kill_at)
            {
                // signal to parant that you can kill me now
                kill(getppid(), SIGUSR1);
            }

            cnt++;
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

        for (size_t cnt = 0; cnt <= FLAGS_kill_at; ++cnt)
        {
            std::string key = std::to_string(cnt);
            std::string v;
            std::string expect_value;
            if (cnt % 4 == 1)
            {
                CHECK_EQ(engine->get(key, v), kNotFound);
            }
            else if (cnt % 4 == 2)
            {
                CHECK_EQ(engine->get(key, v), kSucc)
                    << "expect to successfully get key " << key;
                CHECK_EQ(v, value2);
            }
            else
            {
                CHECK_EQ(engine->get(key, v), kSucc);
                CHECK_EQ(v, value);
            }
        }
    }
    else
    {
        assert(false);
    }

    LOG(INFO) << "PASS " << argv[0];

    return 0;
};

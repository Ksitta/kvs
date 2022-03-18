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
            engine->put(std::to_string(cnt), std::to_string(cnt));
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

        std::string value;
        size_t cnt = 0;
        while (true)
        {
            std::string key = std::to_string(cnt);
            std::string expect_value = key;
            auto ret = engine->get(key, value);
            if (ret == kSucc)
            {
                CHECK_EQ(expect_value, value);
            }
            else
            {
                CHECK_EQ(ret, kNotFound) << "Unexpect return code " << ret;
                break;
            }
            cnt++;
        }
        CHECK_GT(cnt, FLAGS_kill_at) << "key " << std::to_string(cnt)
                                     << " not found, which results data loss";
    }
    else
    {
        assert(false);
    }

    LOG(INFO) << "PASS " << argv[0];

    return 0;
};

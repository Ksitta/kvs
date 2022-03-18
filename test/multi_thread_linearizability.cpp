#include <assert.h>
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "engine.h"
#include "glog/logging.h"
#include "util/utils.h"

#define KV_CNT 1000
#define TEST_CNT 100000
#define KEY_SIZE 16
#define VALUE_SIZE 16
#define THREAD_NUM 4
#define CONFLICT_KEY 50

using namespace kvs;
using namespace bench;

char v[9024];
std::string key;
std::string vs[THREAD_NUM][KV_CNT];

std::unique_ptr<Engine> engine;

class Tracer
{
public:
    Tracer(const std::string &filename)
    {
        outfile.open(filename, std::ios_base::trunc);
    }
    void start_write(int tid, std::string *value)
    {
        std::lock_guard<std::mutex> lk(mu_);
        log("invoke", "write", value, tid);
    }
    void finish_write(int tid, std::string *value)
    {
        std::lock_guard<std::mutex> lk(mu_);
        log("ok", "write", value, tid);
    }
    void start_read(int tid)
    {
        std::lock_guard<std::mutex> lk(mu_);
        log("invoke", "read", nullptr, tid);
    }
    void finish_read(int tid, std::string *value)
    {
        std::lock_guard<std::mutex> lk(mu_);
        log("ok", "read", value, tid);
    }
    ~Tracer()
    {
        outfile << std::endl;
    }

private:
    void log(const std::string &type,
             const std::string f,
             std::string *value,
             int tid)
    {
        outfile << "{:type :" << type << ", :f :" << f << ", :value "
                << ((value == nullptr || value->empty()) ? "nil" : "V" + *value)
                << ", :process " << tid << "}\n";
    }
    std::mutex mu_;
    std::ofstream outfile;
};
std::string trace_file = "./trace.edn";
Tracer tracer(trace_file);

void test_thread_conflict(int id)
{
    std::string value;

    for (int i = 0; i < TEST_CNT; ++i)
    {
        if (fast_pseudo_rand_int(0, 100) <= 50)
        {
            tracer.start_read(id);
            auto ret = engine->get(key, value);
            tracer.finish_read(id, &value);
            CHECK_EQ(ret, kSucc);
        }
        else
        {
            tracer.start_write(id, &vs[id][i % KV_CNT]);
            auto ret = engine->put(key, vs[id][i % KV_CNT]);
            tracer.finish_write(id, &vs[id][i % KV_CNT]);
            CHECK_EQ(ret, kSucc);
        }
    }
}

int main()
{
    LOG(INFO) << "======== multi thread test ===========";

    std::string engine_path = "./tmp_test";

    engine = std::make_unique<Engine>(engine_path, EngineOptions{});
    printf("open engine_path: %s\n", engine_path.c_str());

    for (int t = 0; t < THREAD_NUM; ++t)
    {
        for (int i = 0; i < KV_CNT; ++i)
        {
            gen_random(v, VALUE_SIZE);
            vs[t][i] = v;
        }
    }

    std::thread ths[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; ++i)
    {
        ths[i] = std::thread(test_thread_conflict, i);
    }
    for (int i = 0; i < THREAD_NUM; ++i)
    {
        ths[i].join();
    }

    LOG(INFO) << "trace generated to " << trace_file;

    return 0;
}

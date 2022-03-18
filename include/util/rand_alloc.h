#pragma once

#include <inttypes.h>

#include "engine.h"
#include "glog/logging.h"
#include "util/zipf.h"
#include "utils.h"

namespace bench
{
class IRandAllocator
{
public:
    virtual kvs::IEngine::Key alloc_key(size_t size) = 0;
    virtual kvs::IEngine::Value alloc_value(size_t size) = 0;
    virtual ~IRandAllocator() = default;

private:
};

class PrintableAllocator : public IRandAllocator
{
public:
    kvs::IEngine::Key alloc_key(size_t size) override
    {
        return gen_rand_key(size);
    }
    kvs::IEngine::Value alloc_value(size_t size) override
    {
        return gen_rand_value(size);
    }

private:
};

class ZipfianAllocator : public IRandAllocator
{
public:
    ZipfianAllocator(unsigned int seed, uint64_t max_key, double skewness)
    {
        mehcached_zipf_init(&state_, max_key, skewness, seed);
    }

    kvs::IEngine::Key alloc_key(size_t size) override
    {
        CHECK_EQ(size, sizeof(uint64_t));
        uint64_t r = mehcached_zipf_next(&state_);
        // LOG(INFO) << "get " << r;
        return Key((char *) &r, sizeof(uint64_t));
    }
    kvs::IEngine::Value alloc_value(size_t size) override
    {
        return gen_rand_value(size);
    }

private:
    struct zipf_gen_state state_;
};

class UniformAllocator : public IRandAllocator
{
public:
    UniformAllocator(uint64_t max_key) : max_key_(max_key)
    {
    }

    kvs::IEngine::Key alloc_key(size_t size) override
    {
        CHECK_EQ(size, sizeof(uint64_t));
        DCHECK_GE(max_key_, 1);
        uint64_t r = fast_pseudo_rand_int(0, max_key_ - 1);
        return Key((char *) &r, sizeof(uint64_t));
    }
    kvs::IEngine::Value alloc_value(size_t size) override
    {
        return gen_rand_value(size);
    }

private:
    uint64_t max_key_{0};
};

class PrefixAllocator : public IRandAllocator
{
public:
    PrefixAllocator(const std::string &prefix) : prefix_(prefix)
    {
    }
    kvs::IEngine::Key alloc_key(size_t size) override
    {
        CHECK_GT(size, prefix_.size());
        return prefix_ + gen_rand_key(size - prefix_.size());
    }
    kvs::IEngine::Value alloc_value(size_t size) override
    {
        return prefix_ + gen_rand_value(size - prefix_.size());
    }

private:
    std::string prefix_;
};

class SetAllocator : public IRandAllocator
{
public:
    SetAllocator(const std::vector<std::string> &keys) : keys_(keys)
    {
    }
    SetAllocator(const std::vector<std::string> &keys,
                 const std::vector<std::string> &values)
        : keys_(keys), values_(values)
    {
    }
    kvs::IEngine::Key alloc_key(size_t size) override
    {
        std::ignore = size;
        return keys_[fast_pseudo_rand_int(0, keys_.size() - 1)];
    }
    kvs::IEngine::Value alloc_value(size_t size) override
    {
        std::ignore = size;
        if (values_.empty())
        {
            return gen_rand_value(size);
        }
        return values_[fast_pseudo_rand_int(0, values_.size() - 1)];
    }

private:
    std::vector<std::string> keys_;
    std::vector<std::string> values_;
};

};  // namespace bench
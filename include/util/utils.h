#pragma once
#ifndef INCLUDE_UTILS_H
#define INCLUDE_UTILS_H

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>

#include "engine.h"

namespace bench
{
using Key = kvs::IEngine::Key;
using Value = kvs::IEngine::Value;

// Notice: TLS object is created only once for each combination of type and
// thread. Only use this when you prefer multiple callers share the same
// instance.
template <class T, class... Args>
inline T &TLS(Args &&... args)
{
    thread_local T _tls_item(std::forward<Args>(args)...);
    return _tls_item;
}

inline std::mt19937 &mt19937_generator()
{
    return TLS<std::mt19937>();
}

inline uint64_t fast_pseudo_rand_int(uint64_t min, uint64_t max)
{
    std::uniform_int_distribution<uint64_t> dist(min, max);
    return dist(mt19937_generator());
}

inline uint64_t fast_pseudo_rand_int(uint64_t max)
{
    return fast_pseudo_rand_int(0, max);
}

inline uint64_t fast_pseudo_rand_int()
{
    return fast_pseudo_rand_int(0, std::numeric_limits<uint64_t>::max());
}

/**
 * @brief very fast, at the expense of greater state storage and sometimes less
 * desirable spectral characteristics
 *
 * @return std::ranlux48_base&
 */
inline std::ranlux48_base &ranlux48_base_generator()
{
    return TLS<std::ranlux48_base>();
}
inline double fast_pseudo_rand_dbl(double min, double max)
{
    std::uniform_real_distribution<double> dist(min, max);
    return dist(ranlux48_base_generator());
}
inline double fast_pseudo_rand_dbl(double max)
{
    return fast_pseudo_rand_dbl(0, max);
}
inline double fast_pseudo_rand_dbl()
{
    return fast_pseudo_rand_dbl(0, std::numeric_limits<double>::max());
}

inline bool probability_yes(double prob)
{
    size_t r = fast_pseudo_rand_int(0, 1000 - 1);
    size_t okay = 1.0 * 1000 * prob;
    return r < okay;
}

thread_local unsigned int rand_seed = fast_pseudo_rand_int();
inline void gen_random(char *s, const int len)
{
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i)
    {
        s[i] = alphanum[rand_r(&rand_seed) % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

Key gen_rand_key(size_t size)
{
    std::string ret(size, ' ');
    gen_random(ret.data(), size);
    return ret;
}
Value gen_rand_value(size_t size)
{
    std::string ret(size, ' ');
    gen_random(ret.data(), size);
    return ret;
}

std::pair<Key, Value> gen_rand_kv(size_t k_size, size_t v_size)
{
    return {gen_rand_key(k_size), gen_rand_value(v_size)};
}

}  // namespace bench

#endif
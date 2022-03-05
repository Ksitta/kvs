#pragma once
#ifndef INCLUDE_UTILS_H
#define INCLUDE_UTILS_H

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "engine.h"

namespace bench
{
using Key = kvs::IEngine::Key;
using Value = kvs::IEngine::Value;

inline void color_print(const std::string &s)
{
    printf("\033[1;32;40m%s\033[0m\n", s.c_str());
}

inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long) lo) | (((unsigned long long) hi) << 32);
}

thread_local unsigned int rand_seed = asm_rdtsc();
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
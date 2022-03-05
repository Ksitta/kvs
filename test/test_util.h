#ifndef __ASM_H__
#define __ASM_H__

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

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
    assert(len >= 0);
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
// to guarantee that each string is unique,
// attach mark to the key.
inline void gen_marked_random(char *s, const std::string &mark, size_t len)
{
    assert(mark.length() < len);
    memcpy(s, mark.data(), mark.length());
    gen_random(s + mark.length(), len - mark.length());
}

inline int rand_int(int lower, int upper)
{
    return rand() % (upper - lower + 1) + lower;
}

#endif /* __ASM_H__ */

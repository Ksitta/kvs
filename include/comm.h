#ifndef __COMM_H__
#define __COMM_H__

#include <functional>
#include <string>
#include <mutex>

const int FILE_HEADER = 16;
const int BFSIZE = 10289;
const int MAXSIZE = 1 << 22;  // 4MB

struct KeyOffset
{
    std::string key;
    int offset;
    int len;

    KeyOffset(const std::string &key, int offset, int len)
        : key(key), offset(offset), len(len)
    {
    }
    KeyOffset()
    {
    }
};

#endif
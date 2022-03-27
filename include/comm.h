#ifndef __COMM_H__
#define __COMM_H__

#include <string>

const int FILE_HEADER = 16;
const int BFSIZE = 10240;
const int MAXSIZE = 1 << 21;  // 2MB

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
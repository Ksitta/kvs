#ifndef __COMM_H__
#define __COMM_H__

#include <string>

const int FILE_HEADER = 16;
const int BFSIZE = 10240;

struct KeyOffset
{
    std::string key;
    int offset;
    int len;

    KeyOffset(std::string &key, int offset, int len)
        : key(key), offset(offset), len(len)
    {
    }
    KeyOffset()
    {
    }
};

#endif
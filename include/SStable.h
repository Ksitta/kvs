#ifndef __SSTABLE_H__
#define __SSTABLE_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <list>
#include <string>
#include <vector>

#include "MurmurHash3.h"
#include "const.h"

const uint32_t BFSIZE = 10240;

class BloomFilter
{
public:
    char data[BFSIZE];

    BloomFilter()
    {
        memset(data, 0, BFSIZE);
    }

    void put(std::string &key)
    {
        uint32_t hash[4] = {0};
        MurmurHash3_x64_128(key.c_str(), key.size(), 1, hash);
        for (uint32_t item : hash)
        {
            setbit(item);
        }
    }

    inline void setbit(uint32_t index)
    {
        uint32_t byteindex = index % BFSIZE;
        uint32_t bitindex = index % 8;
        data[byteindex] = (1 << bitindex) || data[byteindex];
    }

    inline bool getbit(uint32_t index)
    {
        uint32_t byteindex = index % BFSIZE;
        uint32_t bitindex = index % 8;
        return (1 << bitindex) && data[byteindex];
    }

    bool test(const std::string &key)
    {
        uint32_t hash[4] = {0};
        MurmurHash3_x64_128(key.c_str(), key.size(), 1, hash);
        for (uint32_t item : hash)
        {
            if (!getbit(item))
            {
                return false;
            }
        }
        return true;
    }
};

class SStable
{
private:
    FILE *file;

public:
    struct KeyOffset
    {
        std::string key;
        int offset;
        int len;

        KeyOffset(std::string &key, int offset, int len)
            : key(key), offset(offset), len(len)
        {
        }
    };
    int num;
    int key_start;
    uint64_t timestamp;
    std::string dir;
    std::string _max;
    std::string _min;

    BloomFilter bloomFilter;
    std::vector<KeyOffset> keypairs;

    const int header_offset = FILE_HEADER;  // timestamp, BloomFilter, num

    SStable(std::string &dir) : dir(dir)
    {
        file = fopen(dir.c_str(), "rb+");
        fread(&timestamp, 8, 1, file);
        fread(&num, 4, 1, file);
        fread(bloomFilter.data, 1, BFSIZE, file);
        int len;
        int offset_len[2];
        key_start = header_offset;
        for (int i = 0; i < num; i++)
        {
            fread(&len, 4, 1, file);
            std::string str;
            str.resize(len);
            fread((void *) str.data(), 1, len, file);
            fread(offset_len, 4, 2, file);
            keypairs.emplace_back(str, offset_len[0], offset_len[1]);
            key_start += len + 12;
        }
        for (int i = 0; i < num; i++)
        {
            std::string val;
            readfile(keypairs[i].offset, keypairs[i].len, val);
        }
        _min = keypairs[0].key;
        _max = keypairs[keypairs.size() - 1].key;
    }

    SStable(std::string &dir,
            uint64_t timestamp,
            int num,
            std::vector<std::pair<std::string, int>> *k_vsize)
        : num(num), timestamp(timestamp), dir(dir)
    {
        file = fopen(dir.c_str(), "wb+");
        int relative_offset = 0;
        _min = (*k_vsize)[0].first;
        _max = (*k_vsize)[num - 1].first;
        key_start = header_offset;
        for (int i = 0; i < num; ++i)
        {
            std::string &key = (*k_vsize)[i].first;
            int length = (*k_vsize)[i].second;
            bloomFilter.put(key);
            keypairs.emplace_back(key, relative_offset, length);
            relative_offset += length;
            key_start += key.size() + 12;
        }
    }

    ~SStable()
    {
        fclose(file);
    }

    void toFile(std::vector<std::string> *values)
    {
        fseek(file, 0, SEEK_SET);
        fwrite(&timestamp, 8, 1, file);
        fwrite(&num, 4, 1, file);
        fwrite(bloomFilter.data, 1, BFSIZE, file);
        for (auto &pair : keypairs)
        {
            int len = pair.key.size();
            fwrite(&len, 4, 1, file);
            fwrite(pair.key.c_str(), 1, len, file);
            fwrite(&pair.offset, 4, 2, file);
        }
        for (auto &value : *values)
        {
            fwrite(value.c_str(), 1, value.size(), file);
        }
        fflush(file);
    }

    void toFile(std::list<std::string> *values)
    {
        fseek(file, 0, SEEK_SET);
        fwrite(&timestamp, 8, 1, file);
        fwrite(&num, 4, 1, file);
        fwrite(bloomFilter.data, 1, BFSIZE, file);
        for (auto &pair : keypairs)
        {
            int len = pair.key.size();
            fwrite(&len, 4, 1, file);
            fwrite(pair.key.c_str(), 1, len, file);
            fwrite(&pair.offset, 4, 2, file);
        }
        for (auto &value : *values)
        {
            fwrite(value.c_str(), 1, value.size(), file);
        }
        fflush(file);
    }

    bool get(const std::string &key, std::string &value)
    {
        int left = 0;
        int right = keypairs.size();
        while (left < right)
        {
            int mid = left + (right - left) / 2;
            if (keypairs[mid].key == key)
            {
                if (keypairs[mid].len == 0)
                {
                    value = "";
                    return true;
                }
                readfile(keypairs[mid].offset, keypairs[mid].len, value);
                return true;
            }
            else if (keypairs[mid].key < key)
            {
                left = mid + 1;
            }
            else
            {
                right = mid;
            }
        }
        return false;
    }

    //从文件中获取value offset处的value
    void readfile(int offset, int len, std::string &value)
    {
        fseek(file, offset + key_start, SEEK_SET);
        value.resize(len);
        fread((void *) value.data(), 1, len, file);
    }

    inline std::vector<KeyOffset> &get_keys()
    {
        return this->keypairs;
    }

    void get_values(std::vector<std::string> &values)
    {
        fseek(file, this->key_start, SEEK_SET);
        for (const auto &item : keypairs)
        {
            std::string value;
            value.resize(item.len);
            fread((void *) value.data(), 1, item.len, file);
            values.emplace_back(value);
        }
    }
};

#endif  //__SSTABLE_H__

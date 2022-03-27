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
#include "comm.h"

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
    int num;
    int type;
    uint64_t timestamp;
    std::string dir;
    std::string _max;
    std::string _min;

    BloomFilter bloomFilter;
    std::vector<KeyOffset> keypairs;

    class Compare
    {
    public:
        bool operator()(const SStable *&a, const SStable *&b)
        {
            return a->timestamp < b->timestamp;
        }
    };

    static bool comp(SStable *a, SStable *b)
    {
        return a->timestamp < b->timestamp;
    }

    SStable(std::string &dir) : dir(dir)
    {
        file = fopen((dir + ".meta").c_str(), "rb");
        fread(&timestamp, 8, 1, file);
        fread(&num, 4, 1, file);
        fread(&type, 4, 1, file);
        if (type == 0)
        {
            int len[2];
            int pos = 0;
            for (int i = 0; i < num; i++)
            {
                fread(len, 4, 2, file);
                std::string str;
                str.resize(len[0]);
                fread((void *) str.data(), 1, len[0], file);
                bloomFilter.put(str);
                keypairs.emplace_back(str, pos, len[1]);
                pos += len[1];
            }
        }
        else
        {
            int len[3];
            for (int i = 0; i < num; i++)
            {
                fread(len, 4, 3, file);
                std::string str;
                str.resize(len[0]);
                fread((void *) str.data(), 1, len[0], file);
                bloomFilter.put(str);
                keypairs.emplace_back(str, len[1], len[2]);
            }
        }
        _min = keypairs[0].key;
        _max = keypairs[keypairs.size() - 1].key;
        fclose(file);
        file = fopen((dir + ".data").c_str(), "rb");
    }

    SStable(std::string &dir,
            uint64_t timestamp,
            int num,
            int type,
            std::vector<KeyOffset> &keypairs,
            FILE *file)
        : file(file),
          num(num),
          type(type),
          timestamp(timestamp),
          dir(dir),
          keypairs(keypairs)
    {
        _min = keypairs[0].key;
        _max = keypairs[num - 1].key;
    }

    ~SStable()
    {
        fclose(file);
    }

    void indextoFile() const
    {
        FILE *tmp = fopen((dir + ".meta").c_str(), "wb+");
        fwrite(&timestamp, 8, 1, tmp);
        fwrite(&num, 4, 1, tmp);
        fwrite(&type, 4, 1, tmp);
        if (type == 0)
        {
            for (int i = 0; i < keypairs.size(); i++)
            {
                int len[2] = {keypairs[i].key.size(), keypairs[i].len};
                fwrite(&len, 4, 2, tmp);
                fwrite(keypairs[i].key.c_str(), 1, len[0], tmp);
            }
        }
        else
        {
            for (int i = 0; i < keypairs.size(); i++)
            {
                int len[3] = {keypairs[i].key.size(),
                              keypairs[i].offset,
                              keypairs[i].len};
                fwrite(&len, 4, 3, tmp);
                fwrite(keypairs[i].key.c_str(), 1, len[0], tmp);
            }
        }
        fclose(tmp);
    }

    void toFile(std::vector<std::string> &values)
    {
        // index
        indextoFile();
        // value
        file = fopen((dir + ".data").c_str(), "wb+");
        for (int i = 0; i < values.size(); i++)
        {
            fwrite(values[i].c_str(), 1, values[i].size(), file);
        }
        fflush(file);
    }

    bool get(const std::string &key, std::string &value) const
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
    void readfile(int offset, int len, std::string &value) const
    {
        fseek(file, offset, SEEK_SET);
        value.resize(len);
        fread((void *) value.data(), 1, len, file);
    }

    inline std::vector<KeyOffset> &get_keys()
    {
        return this->keypairs;
    }

    void get_values(std::vector<std::string> &values) const
    {
        if (type == 0)
        {
            fseek(file, 0, SEEK_SET);
            for (const auto &item : keypairs)
            {
                std::string value;
                value.resize(item.len);
                fread((void *) value.data(), 1, item.len, file);
                values.emplace_back(value);
            }
        }
        else
        {
            for (const auto &item : keypairs)
            {
                std::string value;
                value.resize(item.len);
                fseek(file, item.offset, SEEK_SET);
                fread((void *) value.data(), 1, item.len, file);
                values.emplace_back(value);
            }
        }
    }
};

#endif  //__SSTABLE_H__

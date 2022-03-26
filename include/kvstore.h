#pragma once

#include <list>
#include <map>
#include <queue>
#include <string>

#include "MemTable.h"
#include "MurmurHash3.h"
#include "SStable.h"
#include "comm.h"
#include "utils.h"

class KVStore
{
private:
    std::string dir;
    MemTable *memtab;
    std::vector<std::list<SStable *>> sstables;
    uint64_t timestamp;
    int level;
    int index;

    struct prinode
    {
        uint64_t stamp;
        int pos;

        bool operator<(prinode b) const
        {
            return stamp > b.stamp;  //结构体中，key小的优先级高
        }
    };

public:
    KVStore(const std::string &dir);

    ~KVStore();

    inline std::string getpath(int n)
    {
        std::string path = dir + "/level_" + std::to_string(n);
        return path;
    }

    void put(const std::string &key, const std::string &s);

    bool get(const std::string &key, std::string &value);

    inline bool del(const std::string &key)
    {
        std::string tmp;
        if (get(key, tmp))
        {
            put(key, "");
            return true;
        }
        return false;
    }

    void reset();

    void toSStable();

    void MemTableToSSTable();

    void compaction(int des);

    uint64_t merge(std::vector<SStable *> &needMerge,
                   std::vector<std::string> &keys,
                   std::vector<std::string> &values,
                   int level);

    void allocToSStable(std::vector<std::string> &keys,
                        std::vector<std::string> &vals,
                        int des,
                        uint64_t stamp,
                        std::string &path);
};

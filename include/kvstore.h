#pragma once

#include <algorithm>
#include <list>
#include <queue>
#include <string>
#include <set>
#include <unordered_set>

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
    int level;
    int index;
    uint64_t timestamp;

public:
    std::set<std::string> all_keys;
    KVStore(const std::string &dir);

    ~KVStore();

    void gc();

    inline std::string getpath(int n) const
    {
        std::string path = dir + "/level_" + std::to_string(n);
        return path;
    }

    void put(const std::string &key, const std::string &s);

    bool get(const std::string &key, std::string &value) const;

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

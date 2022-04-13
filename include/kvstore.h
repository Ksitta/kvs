#pragma once

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <queue>
#include <set>
#include <string>
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
    std::atomic<int> ref_cnt;
    std::atomic<int> *ref;

public:
    KVStore(const std::string &dir);

    KVStore()
    {
    }

    ~KVStore();

    void gc();

    inline std::string getpath(int n) const
    {
        std::string path = dir + "/level_" + std::to_string(n);
        return path;
    }

    void visit(const std::string &lower,
               const std::string &upper,
               const std::function<void(const std::string &,
                                        const std::string &)> &visitor);

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

    void MemTableToSSTable();

    void compaction(int des);

    KVStore *snapshot();

    uint64_t merge(std::vector<SStable *> &need_merge,
                   std::vector<std::string> &keys,
                   std::vector<std::string> &values);

    void allocToSStable(std::vector<std::string> &keys,
                        std::vector<std::string> &vals,
                        int des,
                        uint64_t stamp,
                        std::string &path);
};

#include "kvstore.h"

#include <algorithm>
#include <cassert>
#include <queue>

unsigned int string_to_unsigned_int(std::string str)
{
    unsigned int result(0);
    //从字符串首位读取到末位（下标由0到str.size() - 1）
    for (int i = str.size() - 1; i >= 0; i--)
    {
        unsigned int temp(0), k = str.size() - i - 1;
        //判断是否为数字
        if (isdigit(str[i]))
        {
            //求出数字与零相对位置
            temp = str[i] - '0';
            while (k--)
                temp *= 10;
            result += temp;
        }
        else
        {
            break;
        }
    }
    return result;
}

KVStore::KVStore(const std::string &dir) : level(0), index(0), timestamp(0)
{
    this->dir = dir;
    if (!utils::dirExists(dir))
    {
        utils::mkdir(dir.c_str());
        auto path = getpath(0);
        utils::mkdir(path.c_str());
        std::string name = path + "/" + std::to_string(index);
        memtab = new MemTable(name);
        auto add_ss = new std::list<SStable *>;
        sstables.push_back(*add_ss);
        timestamp++;
        index++;
    }
    else
    {
        std::vector<std::string> ret;
        utils::scanDir(dir, ret);  // ret中为所有level的name
        if (ret.size() > 0)
        {
            int max_level =
                string_to_unsigned_int((*(ret.end() - 1)).substr(6));
            for (const auto &item : ret)
            {
                int tmp_max_level = string_to_unsigned_int(item.substr(6));
                if (max_level < tmp_max_level)
                {
                    max_level = tmp_max_level;
                }
            }
            sstables.resize(max_level + 1);
        }
        else
        {
            auto add_ss = new std::list<SStable *>;
            sstables.push_back(*add_ss);
        }

        for (const auto &item : ret)
        {  // item
            std::vector<std::string> sub;
            utils::scanDir(dir + "/" + item, sub);
            uint32_t tmp_level = string_to_unsigned_int(item.substr(6));
            if (tmp_level > level)
                level = tmp_level;
            for (const auto &sst : sub)
            {
                if (sst.substr(sst.size() - 5) != ".meta")
                {
                    continue;
                }
                std::string filename =
                    dir + "/" + item + "/" + sst.substr(0, sst.size() - 5);
                SStable *sstable = new SStable(filename);
                sstables[tmp_level].push_back(sstable);
                uint32_t tmp_index =
                    string_to_unsigned_int(sst.substr(0, sst.size() - 5));
                if (tmp_index > index)
                    index = tmp_index;
                if (sstable->timestamp > timestamp)
                    timestamp = sstable->timestamp;
            }
        }
        index++;
        timestamp++;

        auto path = getpath(0);
        if (!utils::dirExists(path))
        {
            utils::mkdir(path.c_str());
        }

        std::string name = path + "/" + std::to_string(index);
        memtab = new MemTable(name);
    }
}

KVStore::~KVStore()
{
    if (memtab->size() > 0)
    {
        MemTableToSSTable();
    }
    for (auto i : sstables)
    {
        while (i.size())
        {
            delete i.front();
            i.pop_front();
        }
    }
}

void KVStore::put(const std::string &key, const std::string &s)
{
    if (!memtab->put(key, s))
    {
        MemTableToSSTable();
        memtab->put(key, s);
    }
}

bool KVStore::get(const std::string &key, std::string &value)
{
    std::string raw = value;
    bool find = false;
    bool ret = memtab->get(key, raw);
    if (ret)
    {
        if (raw == "")
        {
            return false;
        }
        value = raw;
        return ret;
    }
    uint64_t max_stamp = 0;

    if (sstables.empty() || (sstables.size() == 1 && sstables[0].empty()))
    {
        return false;
    }
    for (auto s = sstables[0].begin(); s != sstables[0].end(); s++)
    {
        if ((*s)->_min <= key && (*s)->_max >= key)
        {
            if (max_stamp < (*s)->timestamp)
            {
                if ((*s)->bloomFilter.test(key))
                {
                    ret = (*s)->get(key, raw);
                    if (ret)
                    {
                        max_stamp = (*s)->timestamp;
                        find = true;
                    }
                }
            }
        }
    }

    if (find)
    {
        if (raw == "")
        {
            return false;
        }
        value = raw;
        return true;
    }

    int levels = sstables.size();
    for (int i = 1; i < levels; i++)
    {
        for (const auto &sst : sstables[i])
        {
            //判断key有没有在区间内
            if (sst->_min <= key && sst->_max >= key)
            {
                if (sst->bloomFilter.test(key))
                {
                    ret = sst->get(key, raw);
                    if (ret)
                    {
                        if (raw == "")
                        {
                            return false;
                        }
                        value = raw;
                        return true;
                    }
                }
                break;
            }
        }
    }
    return false;
}

void KVStore::MemTableToSSTable()
{
    if (memtab->size() == 0)
    {
        return;
    }

    std::vector<KeyOffset> keyoffsets;
    Node *keypairs = memtab->getkeypairs();
    while (keypairs)
    {
        keyoffsets.push_back(keypairs->keypair);
        keypairs = keypairs->right;
    }

    auto path = getpath(0);
    if (!utils::dirExists(path))
    {
        utils::mkdir(path.c_str());
    }

    SStable *_sstable = new SStable(
        memtab->dir, timestamp, memtab->size(), 1, keyoffsets, memtab->file);

    sstables[0].push_back(_sstable);
    _sstable->indextoFile();

    index++;
    timestamp++;
    delete memtab;
    std::string name = path + "/" + std::to_string(index);
    memtab = new MemTable(name);
    // 如果当前第level层已经满了 --> 触发compaction
    int des = 0;
    while (sstables[des].size() > (2 << (des)))
    {
        des++;
        compaction(des);
    }
}

uint64_t KVStore::merge(std::vector<SStable *> &needMerge,
                        std::vector<std::string> &keys,
                        std::vector<std::string> &values,
                        int level)
{
    int nums[needMerge.size()];
    uint64_t timestamps[needMerge.size()];
    int used[needMerge.size()];
    uint64_t timestamp = 0;
    std::priority_queue<
        std::pair<std::string, std::pair<int, int>>,
        std::vector<std::pair<std::string, std::pair<int, int>>>,
        std::greater<std::pair<std::string, std::pair<int, int>>>>
        merge_queue;
    std::vector<std::string> all_value[needMerge.size()];
    for (int i = 0; i < needMerge.size(); i++)
    {
        nums[i] = needMerge[i]->get_keys().size();
        used[i] = 0;
        timestamps[i] = needMerge[i]->timestamp;
        if (timestamps[i] > timestamp)
        {
            timestamp = timestamps[i];
        }
        needMerge[i]->get_values(all_value[i]);
    }
    for (int i = 0; i < needMerge.size(); i++)
    {
        merge_queue.push({needMerge[i]->get_keys()[0].key, {timestamps[i], i}});
    }
    while (merge_queue.size())
    {
        auto first = merge_queue.top();
        merge_queue.pop();
        if (keys.size() && keys.back() == first.first)
        {
            int rows = first.second.second;
            values.back() = all_value[rows][used[rows]];
            used[rows]++;
            if (used[rows] != nums[rows])
            {
                merge_queue.push({needMerge[rows]->get_keys()[used[rows]].key,
                                  {timestamps[rows], rows}});
            }
            continue;
        }
        int rows = first.second.second;
        keys.push_back(needMerge[rows]->get_keys()[used[rows]].key);
        values.push_back(all_value[rows][used[rows]]);
        used[rows]++;
        if (used[rows] != nums[rows])
        {
            merge_queue.push({needMerge[rows]->get_keys()[used[rows]].key,
                              {timestamps[rows], rows}});
        }
    }
    return timestamp;
}

void KVStore::allocToSStable(std::vector<std::string> &keys,
                             std::vector<std::string> &vals,
                             int des,
                             uint64_t stamp,
                             std::string &path)
{
    // find pos to insert
    auto insertIterator = sstables[des].begin();
    for (; insertIterator != sstables[des].end(); ++insertIterator)
    {
        if ((*insertIterator)->timestamp > stamp)
            break;
    }
    uint64_t filesize = FILE_HEADER;
    std::vector<KeyOffset> writeKeys;
    std::vector<std::string> writeValues;
    auto key_num = keys.size();
    int offset = 0;
    for (int i = 0; i < key_num; ++i)
    {
        if (filesize + 8 + keys[i].size() + vals[i].size() > MAXSIZE)
        {
            std::string filename = path + "/" + std::to_string(index);
            auto sst = new SStable(
                filename, stamp, writeKeys.size(), 0, writeKeys, nullptr);
            sstables[des].insert(insertIterator, sst);
            sst->toFile(writeValues);
            writeKeys.clear();
            writeValues.clear();
            index++;
            filesize = FILE_HEADER;
            offset = 0;
        }
        writeKeys.emplace_back(keys[i], offset, vals[i].size());
        writeValues.push_back(vals[i]);
        offset += vals[i].size();
        filesize += 8 + keys[i].size() + vals[i].size();
    }
    std::string filename = path + "/" + std::to_string(index);
    auto sst =
        new SStable(filename, stamp, writeKeys.size(), 0, writeKeys, nullptr);
    sstables[des].insert(insertIterator, sst);
    sst->toFile(writeValues);
    index++;
}

void KVStore::compaction(int des)
{
    std::vector<SStable *> needMerge;  //所有需要合并的sst
    std::list<SStable *> &lastLevel = sstables[des - 1];  //合并层
    std::string min_key = (*lastLevel.begin())->_min;
    std::string max_key = (*lastLevel.begin())->_max;
    //记录合并层哪些sst要被compaction
    if (des - 1 == 0)
    {
        for (auto iterator = lastLevel.begin(); iterator != lastLevel.end();
             iterator++)
        {
            auto &sst = *iterator;
            if (sst->_min < min_key)
            {
                min_key = sst->_min;
            }
            if (sst->_max > max_key)
            {
                max_key = sst->_max;
            }
            needMerge.push_back(sst);
        }
    }
    else
    {
        int overSize = lastLevel.size() - (2 << (des - 1));
        auto iterator = lastLevel.begin();
        for (int i = 0; i < overSize; ++i)
        {
            auto &sst = *iterator;
            min_key = min(min_key, sst->_min);
            max_key = max(max_key, sst->_max);
            needMerge.push_back(sst);
            ++iterator;
        }
    }
    //记录目标层哪些要被合并
    std::string targetPath = getpath(des);
    std::vector<std::list<SStable *>::iterator> needRemove;
    if (sstables.size() > des)
    {  //如果下一层已经存在
        std::list<SStable *> &nextLevel = sstables[des];
        for (auto it = nextLevel.begin(); it != nextLevel.end(); it++)
        {
            auto thisSSTable = *it;
            if (!((min_key > thisSSTable->_max) ||
                  (max_key < thisSSTable->_min)))
            {
                needMerge.push_back(thisSSTable);
                needRemove.push_back(it);
            }
        }
    }
    else
    {
        std::list<SStable *> *add_ss = new std::list<SStable *>;
        sstables.push_back(*add_ss);
        utils::mkdir(targetPath.c_str());
    }
    std::vector<std::string> keys;
    std::vector<std::string> values;
    uint64_t stamp = merge(needMerge, keys, values, des - 1);
    auto &desLevel = sstables[des];
    std::string tmp;
    for (auto thisIterator : needRemove)
    {
        auto thisSSTable = *thisIterator;
        tmp = thisSSTable->dir;
        delete thisSSTable;
        assert(utils::rmfile((tmp + ".data").c_str()) == 0);
        assert(utils::rmfile((tmp + ".meta").c_str()) == 0);
        desLevel.erase(thisIterator);
    }
    allocToSStable(keys, values, des, stamp, targetPath);
    if (des - 1 == 0)
    {
        for (auto sst : sstables[des - 1])
        {
            tmp = sst->dir;
            delete sst;
            assert(utils::rmfile((tmp + ".data").c_str()) == 0);
            assert(utils::rmfile((tmp + ".meta").c_str()) == 0);
        }
        sstables[des - 1].clear();
    }
    else
    {
        int overSize = sstables[des - 1].size() - (2 << (des - 1));
        for (int i = 0; i < overSize; ++i)
        {
            SStable *sst = sstables[des - 1].front();
            tmp = sst->dir;
            delete sst;
            assert(utils::rmfile((tmp + ".data").c_str()) == 0);
            assert(utils::rmfile((tmp + ".meta").c_str()) == 0);
            sstables[des - 1].pop_front();
        }
    }
}
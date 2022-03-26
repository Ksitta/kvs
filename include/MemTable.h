#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__

#include <cstdint>
#include <string>
#include <vector>

#include "comm.h"

const int MAXSIZE = 1 << 21;  // 2MB

struct Node
{
    Node *right, *down;  // skiplist
    KeyOffset keypair;
    Node(Node *right, Node *down, std::string key, int offset, int len)
        : right(right), down(down), keypair(key, offset, len)
    {
    }
    Node() : right(nullptr), down(nullptr)
    {
    }
};

class MemTable
{
private:
    int total_size;
    int entry_size;
    int offset;

public:
    Node *head;
    FILE *file;
    std::string dir;

    MemTable(std::string &dir) : dir(dir)
    {
        file = fopen((dir + ".data").c_str(), "wb+");
        head = new Node();
        total_size = FILE_HEADER;
        entry_size = 0;
        offset = 0;
    }

    ~MemTable()
    {
        Node *p = head;
        Node *q = p;
        while (q)
        {
            p = q;
            q = q->down;
            while (p)
            {
                Node *tmp = p;
                p = p->right;
                delete tmp;
            }
        }
    }

    int size()
    {
        return entry_size;
    }

    bool get(const std::string &key, std::string &value)
    {
        Node *pos = head;
        while (pos)
        {
            while (pos->right && pos->right->keypair.key < key)
            {
                pos = pos->right;
            }

            if (pos->right && pos->right->keypair.key == key)
            {
                int offset = pos->right->keypair.offset;
                int len = pos->right->keypair.len;
                if (len == 0)
                {
                    value = "";
                    return true;
                }
                value.resize(len);
                fseek(file, offset, SEEK_SET);
                fread((void *) value.data(), 1, len, file);
                return true;
            }
            pos = pos->down;
        }
        return false;
    }

    bool put(const std::string &key, const std::string &val)
    {
        bool cover = false;
        std::vector<Node *> path;  //记录从上到下的路径
        Node *pos = head;
        while (pos)
        {
            while (pos->right && pos->right->keypair.key < key)
            {
                pos = pos->right;
            }
            path.push_back(pos);
            if (pos->right && pos->right->keypair.key == key)
            {
                cover = true;
                break;
            }
            pos = pos->down;
        }
        if (cover)
        {
            Node *edit = path.back()->right;
            int new_size = total_size + val.size();
            if (new_size > MAXSIZE)
            {
                return false;
            }
            while (edit)
            {
                edit->keypair.offset = offset;
                edit->keypair.len = val.size();
                edit = edit->down;
            }
            total_size = new_size;
            fseek(file, offset, SEEK_SET);
            fwrite(val.c_str(), 1, val.size(), file);
            offset += val.size();
            return true;
        }
        if (total_size + val.size() + key.size() + 12 > MAXSIZE)
        {
            return false;
        }

        bool insert_up = true;
        Node *down_node = nullptr;
        while (insert_up && path.size() > 0)
        {  //从下至上搜索路径回溯，50%概率
            Node *insert = path.back();
            path.pop_back();
            insert->right = new Node(insert->right,
                                     down_node,
                                     key,
                                     offset,
                                     val.size());  // add新结点
            down_node = insert->right;  //把新结点赋值为down_node
            insert_up = (rand() & 1);   // 50%概率
        }
        if (insert_up)
        {  //插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, down_node, key, offset, val.size());
            head->down = oldHead;
        }
        entry_size++;
        total_size += (val.size() + key.size() + 12);
        fseek(file, offset, SEEK_SET);
        fwrite(val.c_str(), 1, val.size(), file);
        offset += val.size();
        return true;
    }

    Node *getkeypairs()
    {
        Node *p = head;
        while (p->down)
        {
            p = p->down;
        }
        return p->right;
    }
};

#endif

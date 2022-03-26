#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__

#include <cstdint>
#include <string>
#include <vector>

#include "const.h"

const int MAXSIZE = 1 << 21;  // 2MB

struct Node
{
    Node *right, *down;  // skiplist
    std::string key;
    std::string val;
    Node(Node *right, Node *down, std::string key, std::string val)
        : right(right), down(down), key(key), val(val)
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

public:
    Node *head;

    MemTable()
    {
        head = new Node();  //初始化头结点
        total_size = FILE_HEADER;
        entry_size = 0;
    }

    ~MemTable()
    {
        Node *p = head;
        Node *q = p;
        while (q)
        {
            p = q;
            while (p)
            {
                Node *tmp = p;
                p = p->right;
                delete tmp;
            }
            q = q->down;
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
            while (pos->right && pos->right->key < key)
            {
                pos = pos->right;
            }

            if (pos->right && pos->right->key == key)
            {
                value = pos->right->val;
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
            while (pos->right && pos->right->key < key)
            {
                pos = pos->right;
            }
            path.push_back(pos);
            if (pos && pos->right && pos->right->key == key)
            {
                cover = true;
                break;
            }
            pos = pos->down;
        }
        if (cover)
        {
            Node *edit = path.back()->right;
            if (edit->val == val)
            {
                return true;  //要覆盖的val相同不用改
            }
            int new_size = total_size + val.size() - edit->val.size();
            if (new_size > MAXSIZE)
            {
                return false;
            }
            while (edit)
            {
                edit->val = val;
                edit = edit->down;
            }
            total_size = new_size;
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
            insert->right =
                new Node(insert->right, down_node, key, val);  // add新结点
            down_node = insert->right;  //把新结点赋值为down_node
            insert_up = (rand() & 1);   // 50%概率
        }
        if (insert_up)
        {  //插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, down_node, key, val);
            head->down = oldHead;
        }
        entry_size++;
        total_size += (val.size() + key.size() + 8);
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

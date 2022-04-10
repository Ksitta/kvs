#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__

#include <stdint.h>

#include <string>
#include <vector>

#include "comm.h"

struct Node {
    Node *right, *down; // skiplist
    KeyOffset keypair;
    Node(Node *right, Node *down, const std::string &key, int offset, int len)
        : right(right), down(down), keypair(key, offset, len) {
    }
    Node() : right(nullptr), down(nullptr) {
    }
};

class MemTable {
private:
    int total_size;
    int entry_size;
    int offset;

public:
    Node *head;
    FILE *file;
    FILE *log;
    std::string dir;

    MemTable(const std::string &dir, FILE *log, bool snap = false) : log(log), dir(dir) {
        if(snap){
            file = fopen((dir + ".data").c_str(), "rb+");
        } else {
            file = fopen((dir + ".data").c_str(), "wb+");
        }
        head = new Node();
        total_size = FILE_HEADER;
        entry_size = 0;
        offset = 0;
        if(!snap){
            recover();
        }
    }

    void clone(MemTable *t){
        Node *node = t->getkeypairs();
        while(node){
            put_clone(node->keypair);
            node = node->right;
        }
    }

    void gc(std::unordered_set<std::string> &removable_keys) {
        Node *node = getkeypairs();
        while (node) {
            removable_keys.insert(node->keypair.key);
            node = node->right;
        }
    }

    ~MemTable() {
        Node *p = head;
        Node *q = p;
        while (q) {
            p = q;
            q = q->down;
            while (p) {
                Node *tmp = p;
                p = p->right;
                delete tmp;
            }
        }
        if(log){
            fclose(log);
        }
    }

    int size() const {
        return entry_size;
    }

    bool get(const std::string &key, std::string &value) const {
        Node *pos = head;
        while (pos) {
            while (pos->right && pos->right->keypair.key < key) {
                pos = pos->right;
            }

            if (pos->right && pos->right->keypair.key == key) {
                int off = pos->right->keypair.offset;
                int len = pos->right->keypair.len;
                if (len == 0) {
                    value = "";
                    return true;
                }
                value.resize(len);
                fseek(file, off, SEEK_SET);
                std::ignore = fread((void *)value.data(), 1, len, file);
                return true;
            }
            pos = pos->down;
        }
        return false;
    }

    void flush(){
        fflush(file);
    }

    void visit(const std::string &lower,
               const std::string &upper,
               const std::function<void(const std::string &,
                                        const std::string &)> &visitor,
               std::unordered_set<std::string> &used_key) const {
        bool lb = false, ub = false;
        if (lower == "") {
            lb = true;
        }
        if (upper == "") {
            ub = true;
        }
        Node *node = getkeypairs();
        while (node) {
            if ((lb || lower <= node->keypair.key) && (ub || upper >= node->keypair.key)) {
                used_key.insert(node->keypair.key);
                if (node->keypair.len == 0) {
                    node = node->right;
                    continue;
                }
                std::string val;
                val.resize(node->keypair.len);
                fseek(file, node->keypair.offset, SEEK_SET);
                std::ignore = fread((void *)val.data(), 1, node->keypair.len, file);
                visitor(node->keypair.key, val);
            }
            node = node->right;
        }
    }

    bool put(const std::string &key, const std::string &val) {
        if (total_size + val.size() + key.size() + 12 > MAXSIZE) {
            return false;
        }
        fseek(file, offset, SEEK_SET);
        std::ignore = fwrite(val.c_str(), 1, val.size(), file);
        int log_size[2] = {int(key.size()), int(val.size())};
        std::ignore = fwrite(log_size, 4, 2, log);
        std::ignore = fwrite(key.c_str(), 1, log_size[0], log);
        std::ignore = fwrite(val.c_str(), 1, log_size[1], log);
        fflush(log);
        bool cover = false;
        std::vector<Node *> path; //记录从上到下的路径
        Node *pos = head;
        while (pos) {
            while (pos->right && pos->right->keypair.key < key) {
                pos = pos->right;
            }
            path.push_back(pos);
            if (pos->right && pos->right->keypair.key == key) {
                cover = true;
                break;
            }
            pos = pos->down;
        }
        if (cover) {
            Node *edit = path.back()->right;
            int new_size = total_size + val.size();
            while (edit) {
                edit->keypair.offset = offset;
                edit->keypair.len = val.size();
                edit = edit->down;
            }
            total_size = new_size;
            offset += val.size();
            return true;
        }

        Node *down_node = nullptr;
        int height = 1;
        int path_size = path.size();
        long rand_num;
        drand48_data randBuffer;
        for (int i = 0; i < path_size; i++) {
            lrand48_r(&randBuffer, &rand_num);
            if (rand_num & 1) {
                height++;
            } else {
                break;
            }
        }
        while (height && path.size() > 0) { //从下至上搜索路径回溯，50%概率
            Node *insert = path.back();
            path.pop_back();
            insert->right = new Node(insert->right,
                                     down_node,
                                     key,
                                     offset,
                                     val.size()); // add新结点
            down_node = insert->right;            //把新结点赋值为down_node
            --height;
        }
        if (height) { //插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, down_node, key, offset, val.size());
            head->down = oldHead;
        }
        entry_size++;
        total_size += (val.size() + key.size() + 12);
        offset += val.size();
        return true;
    }

    bool put_nolog(const std::string &key, const std::string &val) {
        bool cover = false;
        std::vector<Node *> path; //记录从上到下的路径
        Node *pos = head;
        while (pos) {
            while (pos->right && pos->right->keypair.key < key) {
                pos = pos->right;
            }
            path.push_back(pos);
            if (pos->right && pos->right->keypair.key == key) {
                cover = true;
                break;
            }
            pos = pos->down;
        }
        if (cover) {
            Node *edit = path.back()->right;
            int new_size = total_size + val.size();
            if (new_size > MAXSIZE) {
                return false;
            }
            while (edit) {
                edit->keypair.offset = offset;
                edit->keypair.len = val.size();
                edit = edit->down;
            }
            total_size = new_size;
            fseek(file, offset, SEEK_SET);
            std::ignore = fwrite(val.c_str(), 1, val.size(), file);
            offset += val.size();
            return true;
        }
        if (total_size + val.size() + key.size() + 12 > MAXSIZE) {
            return false;
        }

        bool insert_up = true;
        Node *down_node = nullptr;
        while (insert_up && path.size() > 0) { //从下至上搜索路径回溯，50%概率
            Node *insert = path.back();
            path.pop_back();
            insert->right = new Node(insert->right,
                                     down_node,
                                     key,
                                     offset,
                                     val.size()); // add新结点
            down_node = insert->right;            //把新结点赋值为down_node
            insert_up = (rand() & 1);             // 50%概率
        }
        if (insert_up) { //插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, down_node, key, offset, val.size());
            head->down = oldHead;
        }
        entry_size++;
        total_size += (val.size() + key.size() + 12);
        fseek(file, offset, SEEK_SET);
        std::ignore = fwrite(val.c_str(), 1, val.size(), file);
        offset += val.size();
        return true;
    }

    bool put_clone(const KeyOffset &keypairs) {
        bool cover = false;
        std::vector<Node *> path; //记录从上到下的路径
        Node *pos = head;
        while (pos) {
            while (pos->right && pos->right->keypair.key < keypairs.key) {
                pos = pos->right;
            }
            path.push_back(pos);
            if (pos->right && pos->right->keypair.key == keypairs.key) {
                cover = true;
                break;
            }
            pos = pos->down;
        }
        if (cover) {
            Node *edit = path.back()->right;
            int new_size = total_size + keypairs.len;
            if (new_size > MAXSIZE) {
                return false;
            }
            while (edit) {
                edit->keypair.offset = offset;
                edit->keypair.len = keypairs.len;
                edit = edit->down;
            }
            total_size = new_size;
            offset += keypairs.len;
            return true;
        }
        if (total_size + keypairs.len + keypairs.key.size() + 12 > MAXSIZE) {
            return false;
        }

        bool insert_up = true;
        Node *down_node = nullptr;
        while (insert_up && path.size() > 0) { //从下至上搜索路径回溯，50%概率
            Node *insert = path.back();
            path.pop_back();
            insert->right = new Node(insert->right,
                                     down_node,
                                     keypairs.key,
                                     offset,
                                     keypairs.len); // add新结点
            down_node = insert->right;            //把新结点赋值为down_node
            insert_up = (rand() & 1);             // 50%概率
        }
        if (insert_up) { //插入新的头结点，加层
            Node *oldHead = head;
            head = new Node();
            head->right = new Node(nullptr, down_node, keypairs.key, offset, keypairs.len);
            head->down = oldHead;
        }
        entry_size++;
        total_size += (keypairs.len + keypairs.key.size() + 12);
        offset += keypairs.len;
        return true;
    }

    void recover() {
        while (1) {
            int len[2];
            std::string key;
            std::string val;
            std::ignore = fread(len, 4, 2, log);
            if (feof(log)) {
                break;
            }
            key.resize(len[0]);
            val.resize(len[1]);
            std::ignore = fread((void *)key.data(), 1, len[0], log);
            std::ignore = fread((void *)val.data(), 1, len[1], log);
            put_nolog(key, val);
        }
    }

    Node *getkeypairs() const {
        Node *p = head;
        while (p->down) {
            p = p->down;
        }
        return p->right;
    }
};

#endif

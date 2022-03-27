#pragma once
#ifndef INCLUDE_ENGINE_H_
#define INCLUDE_ENGINE_H_
#include <functional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "conf.h"
#include "interfaces.h"
#include "kvstore.h"
#include "options.h"

namespace kvs
{
/**
 * The actual Engine you need to implement
 * TODO: your code
 */
class Engine : public IEngine
{
public:
    Engine(const std::string &path, EngineOptions options);

    static Pointer new_instance(const std::string &path, EngineOptions options)
    {
        return std::make_shared<Engine>(path, options);
    }

    virtual ~Engine();

    // Write a key-value pair into engine
    RetCode put(const Key &key, const Value &value) override;
    RetCode remove(const Key &key) override;
    RetCode get(const Key &key, Value &value) override;

    /**
     * @brief syncing all the modifications to the disk/SSD
     * All the modification operations before calling sync() should be
     * persistent
     */
    RetCode sync() override;

    /**
     * @brief visit applies the visitor to all the KV pairs within
     * the range of [lower, upper).
     * Note that lower == "" is treated as the one before all the keys
     * upper == "" is treated as the one after all the keys
     * So, visit("", "", visitor) will traverse the entire database.
     * @param lower the lower key range
     * @param upper the upper key range
     * @param visitor any function which accepts Key, Value and return void
     *
     * @example
     * void print_visitor(const Key& k, const Value& v)
     * {
     *     std::cout << "visiting key " << k << ", value " << v << std::endl;
     * }
     * engine.visit("a", "Z", print_visitor);
     */
    RetCode visit(const Key &lower,
                  const Key &upper,
                  const Visitor &visitor) override;
    /**
     * @brief generate snapshot, if you want to implement
     * @see class Snapshot
     */
    std::shared_ptr<IROEngine> snapshot() override;

    /**
     * @brief trigger garbage collection, if you want to implement
     */
    RetCode garbage_collect() override;

private:
    KVStore kv;
    std::shared_mutex lock;
};

}  // namespace kvs

#endif  // INCLUDE_ENGINE_H_

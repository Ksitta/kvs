#pragma once
#ifndef KVS_INTERFACE_H_
#define KVS_INTERFACE_H_

#include <functional>
#include <memory>
#include <string>

namespace kvs
{
enum RetCode
{
    kSucc = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kIncomplete = 6,
    kTimedOut = 7,
    kFull = 8,
    kOutOfMemory = 9,
};

/**
 * ReadOnly engine Interface
 */
class IROEngine
{
public:
    // using Key = std::vector<char>;
    // using Value = std::vector<char>;
    using Key = std::string;
    using Value = std::string;
    using Pointer = std::shared_ptr<IROEngine>;

    virtual RetCode get(const Key &key, Value &value) = 0;

    using Visitor = std::function<void(const Key &, const Value &)>;
    virtual RetCode visit(const Key &lower,
                          const Key &upper,
                          const Visitor &visitor) = 0;
    virtual ~IROEngine() = default;
};

/**
 * The engine interface.
 * Discribe what an engine can do
 */
class IEngine : public IROEngine
{
public:
    using Pointer = std::shared_ptr<IEngine>;
    virtual RetCode put(const Key &key, const Value &value) = 0;
    virtual RetCode remove(const Key &key) = 0;
    virtual RetCode get(const Key &key, Value &value) = 0;
    virtual RetCode visit(const Key &lower,
                          const Key &upper,
                          const Visitor &visitor) = 0;
    virtual RetCode sync() = 0;
    virtual std::shared_ptr<IROEngine> snapshot() = 0;
    virtual RetCode garbage_collect() = 0;
};

}  // namespace kvs

#endif
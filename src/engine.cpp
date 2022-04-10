#include "engine.h"

namespace kvs
{

Engine::Engine(const std::string &path, EngineOptions options)
    : kv(path.c_str())
{
    // std::cout << "created" << std::endl;
    // TODO: your code here
    // std::ignore = path;
    std::ignore = options;
}

Engine::~Engine()
{
    // std::cout << "destroyed" << std::endl;
    // TODO: your code here
}

RetCode Engine::put(const Key &key, const Value &value)
{
    // TODO: your code here
    // std::ignore = key;
    // std::ignore = value;
    // std::cout << "put " << key << " " << value << std::endl;
    lock.lock();
    kv.put(key, value);
    lock.unlock();
    return kSucc;
}
RetCode Engine::remove(const Key &key)
{
    // TODO: your code here
    // std::ignore = key;
    // std::cout << "remove " << key << " " << std::endl;
    lock.lock();
    bool ret = kv.del(key);
    lock.unlock();
    if (ret == false)
    {
        return kNotFound;
    }
    return kSucc;
}

RetCode Engine::get(const Key &key, Value &value)
{
    // TODO: your code here
    // std::ignore = key;
    // std::ignore = value;
    // std::cout << "get " << key << std::endl;
    lock.lock();
    bool ret = kv.get(key, value);
    lock.unlock();
    if (ret == false)
    {
        return kNotFound;
    }
    return kSucc;
}

RetCode Engine::sync()
{
    return kSucc;
}

RetCode Engine::visit(const Key &lower, const Key &upper, const Visitor &visitor)
{
    // TODO: your code here
    // std::ignore = lower;
    // std::ignore = upper;
    // std::ignore = visitor;
    // std::cout << "visit" << std::endl;
    lock.lock();
    kv.visit(lower, upper, visitor);
    lock.unlock();
    return kSucc;
}

RetCode Engine::garbage_collect()
{
    lock.lock();
    kv.gc();
    lock.unlock();
    return kSucc;
}

std::shared_ptr<IROEngine> Engine::snapshot()
{
    // TODO: your code here
    // std::cout << "snaped" <<std::endl;
    lock.lock();
    KVStore* ptr = kv.snapshot();
    lock.unlock();
    return std::make_shared<Snap>(ptr);
}

}  // namespace kvs

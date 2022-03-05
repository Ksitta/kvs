#pragma once

#include <thread>

#include "bench.h"
#include "engine.h"
#include "glog/logging.h"
#include "interfaces.h"

namespace bench
{
namespace impl
{
inline void validate_get(const std::map<Key, Value> &kv,
                         kvs::IROEngine::Pointer engine,
                         const TestCase &c)
{
    std::string value;
    auto it = kv.find(c.key);
    if (it != kv.end())
    {
        CHECK_EQ(engine->get(c.key, value), kvs::kSucc)
            << "Getting key " << c.key << " expect kSucc";

        CHECK_EQ(value, it->second)
            << "Getting key " << c.key << " expect value " << it->second
            << ", actual got: " << value;
    }
    else
    {
        CHECK_EQ(engine->get(c.key, value), kvs::kNotFound)
            << "Getting key " << c.key << " expect kNotFound";

        CHECK_EQ(value, std::string()) << "Getting a not-found key " << c.key
                                       << "should not modify the "
                                          "@value field";
    }
}

inline void validate_put(std::map<Key, Value> &kv,
                         kvs::IEngine::Pointer engine,
                         const TestCase &c)
{
    CHECK_EQ(engine->put(c.key, c.value), kvs::kSucc)
        << "Putting key " << c.key << ", value " << c.value << " expect kSucc";
    kv[c.key] = c.value;
}

inline void validate_delete(std::map<Key, Value> &kv,
                            kvs::IEngine::Pointer engine,
                            const TestCase &c)
{
    auto it = kv.find(c.key);
    if (it != kv.end())
    {
        CHECK_EQ(engine->remove(c.key), kvs::kSucc)
            << "Removing key " << c.key << " expect kSucc";
        kv.erase(it);
    }
    else
    {
        CHECK_EQ(engine->remove(c.key), kvs::kNotFound)
            << "Removing key " << c.key << " expect kNotFound";
    }
}

inline void validate_visit(const std::map<Key, Value> &kv,
                           kvs::IROEngine::Pointer engine,
                           const TestCase &c)
{
    size_t got = 0;
    auto visitor = [&got, &c, &kv](const Key &key, const Value &value) {
        auto it = kv.find(key);
        if (it == kv.end())
        {
            LOG(FATAL) << "Visiting in range [" << c.lower << ", " << c.upper
                       << ") found  key " << key << " with value " << value
                       << ", which should not exist";
        }
        CHECK_EQ(it->second, value)
            << "Visiting in range [" << c.lower << ", " << c.upper
            << ") found key " << key << " with unexpected value " << value
            << ", expect " << it->second;
        got++;
    };

    CHECK_EQ(engine->visit(c.lower, c.upper, visitor), kvs::kSucc)
        << "Visiting in range [" << c.lower << ", " << c.upper
        << ") expect kSucc";

    typename std::map<Key, Value>::const_iterator beg_it;
    typename std::map<Key, Value>::const_iterator end_it;

    if (c.lower == Key())
    {
        beg_it = kv.begin();
    }
    else
    {
        beg_it = kv.lower_bound(c.lower);
    }
    if (c.upper == Key())
    {
        end_it = kv.end();
    }
    else
    {
        end_it = kv.upper_bound(c.upper);
    }
    auto diff = std::distance(beg_it, end_it);
    CHECK_EQ(diff, got) << "Visiting in range [" << c.lower << ", " << c.upper
                        << ") expect get " << diff << " records. Actual get "
                        << got;
}

}  // namespace impl

class EngineValidator;
class SnapshotValidator
{
public:
    SnapshotValidator(const std::map<Key, Value> &kv,
                      kvs::IROEngine::Pointer ro_engine)
        : kv_(kv), engine_(ro_engine)
    {
    }
    void validate_execute(const std::vector<TestCase> &bench)
    {
        for (const auto &c : bench)
        {
            switch (c.op)
            {
            case Op::kGet:
            {
                impl::validate_get(kv_, engine_, c);
                break;
            }
            case Op::kVisit:
            {
                impl::validate_visit(kv_, engine_, c);
                break;
            }
            case Op::kNoOp:
            {
                break;
            }
            default:
            {
                LOG(FATAL) << "invalid op " << (int) c.op
                           << " for a read-only engine";
            }
            }
        }
    }

private:
    std::map<Key, Value> kv_;
    kvs::IROEngine::Pointer engine_;
};

class EngineValidator
{
public:
    EngineValidator(kvs::IEngine::Pointer engine) : engine_(engine)
    {
    }
    EngineValidator(kvs::IEngine::Pointer engine,
                    const std::map<Key, Value> &kv)
        : engine_(engine), kv_(kv)
    {
    }
    void execute(const std::vector<TestCase> &bench,
                 size_t from_idx = 0,
                 size_t limit = std::numeric_limits<size_t>::max())
    {
        size_t end = std::min(bench.size(), from_idx + limit);

        for (size_t i = from_idx; i < end; ++i)
        {
            const auto &c = bench[i];
            switch (c.op)
            {
            case Op::kPut:
            {
                auto ret = engine_->put(c.key, c.value);
                CHECK_EQ(ret, kvs::kSucc);
                break;
            }
            case Op::kDelete:
            {
                auto ret = engine_->remove(c.key);
                CHECK(ret == kvs::kSucc || ret == kvs::kNotFound);
                break;
            }
            case Op::kGet:
            {
                std::string value;
                auto ret = engine_->get(c.key, value);
                CHECK(ret == kvs::kSucc || ret == kvs::kNotFound);
                break;
            }
            case Op::kGc:
            {
                CHECK_EQ(engine_->garbage_collect(), kvs::kSucc)
                    << "Failed to gc.";
                break;
            }
            case Op::kSnapshot:
            {
                auto ss = CHECK_NOTNULL(engine_->snapshot());
                break;
            }
            case Op::kSync:
            {
                CHECK_EQ(engine_->sync(), kvs::kSucc);
                break;
            }
            case Op::kVisit:
            {
                engine_->visit(
                    c.lower, c.upper, [](const Key &, const Value &) {});
                break;
            }
            case Op::kNoOp:
            {
                break;
            }
            default:
            {
                LOG(FATAL) << "invalid op " << (int) c.op;
            }
            }
        }
    }

    void validate_execute(const std::vector<TestCase> &bench)
    {
        for (const auto &c : bench)
        {
            switch (c.op)
            {
            case Op::kPut:
            {
                impl::validate_put(kv_, engine_, c);
                break;
            }
            case Op::kDelete:
            {
                impl::validate_delete(kv_, engine_, c);
                break;
            }
            case Op::kGet:
            {
                impl::validate_get(kv_, engine_, c);
                break;
            }
            case Op::kGc:
            {
                CHECK_EQ(engine_->garbage_collect(), kvs::kSucc)
                    << "Failed to gc.";
                break;
            }
            case Op::kSnapshot:
            {
                auto ss = CHECK_NOTNULL(engine_->snapshot());
                break;
            }
            case Op::kSync:
            {
                CHECK(engine_->sync());
                break;
            }
            case Op::kVisit:
            {
                impl::validate_visit(kv_, engine_, c);
                break;
            }
            case Op::kNoOp:
            {
                break;
            }
            default:
            {
                LOG(FATAL) << "invalid op " << (int) c.op;
            }
            }
        }
    }
    SnapshotValidator snapshot_validator()
    {
        return SnapshotValidator(kv_, CHECK_NOTNULL(engine_->snapshot()));
    }
    const std::map<Key, Value> &get_internal_kv() const
    {
        return kv_;
    }

private:
    kvs::IEngine::Pointer engine_;

    std::map<Key, Value> kv_;
};

}  // namespace bench
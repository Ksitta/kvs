#pragma once

#include <cstdlib>
#include <map>
#include <set>
#include <vector>

#include "glog/logging.h"
#include "util/rand_alloc.h"
#include "utils.h"

namespace bench
{
enum class Op
{
    kPut,
    kDelete,
    kGet,
    kSync,
    kGc,
    kSnapshot,
    kVisit,
    kNoOp,
};

inline std::ostream &operator<<(std::ostream &os, const Op &op)
{
    switch (op)
    {
    case Op::kPut:
    {
        os << "put";
        break;
    }
    case Op::kDelete:
    {
        os << "delete";
        break;
    }
    case Op::kGet:
    {
        os << "get";
        break;
    }
    case Op::kSync:
    {
        os << "sync";
        break;
    }
    case Op::kGc:
    {
        os << "gc";
        break;
    }
    case Op::kSnapshot:
    {
        os << "snapshot";
        break;
    }
    case Op::kVisit:
    {
        os << "visit";
        break;
    }
    case Op::kNoOp:
    {
        os << "no-op";
    }
    default:
        LOG(FATAL) << "Invalid op " << (int) op;
    }
    return os;
}

struct TestCase
{
    Op op;
    bench::Key key;
    bench::Value value;
    // for visitor
    bench::Key lower;
    bench::Key upper;
};

inline std::ostream &operator<<(std::ostream &os, const TestCase &c)
{
    os << "{Op: " << c.op << ", key: " << c.key << ", val: " << c.value
       << ", lower: " << c.lower << ", upper: " << c.upper << "}";
    return os;
}

template <typename T>
inline auto random_choose(
    const std::set<T> &set,
    size_t rand_limit = std::numeric_limits<size_t>::max())
{
    auto it = set.begin();
    size_t limit = std::min(set.size(), rand_limit);
    auto adv = fast_pseudo_rand_int(0, limit - 1);
    std::advance(it, adv);
    return *it;
}

struct OpConfig
{
    Op op;
    // the rate of generating this op
    double op_rate;
    size_t key_size_limit;
    size_t value_size_limit;
    // the prabibility that the key will be found
    double found_rate;
    // the percentage of range length that the visit will require
    double visit_range_percent;
};

inline std::ostream &operator<<(std::ostream &os, const OpConfig &conf)
{
    os << "{Op: " << conf.op << ", rate: " << conf.op_rate
       << ", ks: " << conf.key_size_limit << ", vs: " << conf.value_size_limit
       << ", found_rate: " << conf.found_rate
       << ", visit rng: " << conf.visit_range_percent << "}";
    return os;
}

class BenchGenerator
{
public:
    BenchGenerator(const std::vector<OpConfig> &op_conf,
                   std::shared_ptr<IRandAllocator> rand_allocator)
        : op_confs_(op_conf), allocator_(rand_allocator)
    {
        validate();

        double acc = 0;
        for (const auto &opconf : op_confs_)
        {
            acc += opconf.op_rate;
            acc_prob_to_conf_[acc] = opconf;
        }
    }

    void validate()
    {
        double sum_ratio = 0;
        for (const auto &opconf : op_confs_)
        {
            sum_ratio += opconf.op_rate;
        }
        CHECK_LT(std::abs(1.0 - sum_ratio), 0.001)
            << "sum_ratio does not sum up to 1.";
    }

    std::vector<TestCase> rand_tests(size_t test_size, size_t sync_every)
    {
        std::vector<TestCase> ret;
        ret.reserve(test_size);

        for (size_t i = 0; i < test_size; ++i)
        {
            Op op;
            double found_rate = 0;
            size_t key_size_limit = 0;
            size_t value_size_limit = 0;
            double visit_range_percent = 0;

            if ((sync_every != 0) && (i % sync_every == 0))
            {
                op = Op::kSync;
                found_rate = 0;
                key_size_limit = 0;
                value_size_limit = 0;
                visit_range_percent = 0;
            }
            else
            {
                auto r = fast_pseudo_rand_dbl(0, 1);
                auto it = acc_prob_to_conf_.lower_bound(r);
                if (it == acc_prob_to_conf_.end())
                {
                    LOG(FATAL) << "Failed to generate op from r " << r;
                }
                op = it->second.op;

                found_rate = it->second.found_rate;
                key_size_limit = it->second.key_size_limit;
                value_size_limit = it->second.value_size_limit;
                visit_range_percent = it->second.visit_range_percent;
            }
            ret.emplace_back();
            ret.back().op = op;

            switch (op)
            {
            case Op::kPut:
            {
                Key key;
                if (probability_yes(found_rate) && !inserted_keys_.empty())
                {
                    key = random_choose(inserted_keys_, 10000);
                }
                else
                {
                    key = allocator_->alloc_key(key_size_limit);
                }

                ret.back().key = key;
                ret.back().value = allocator_->alloc_value(value_size_limit);
                inserted_keys_.insert(key);
                break;
            }
            case Op::kDelete:
            {
                Key key;
                if (probability_yes(found_rate) && !inserted_keys_.empty())
                {
                    key = random_choose(inserted_keys_, 10000);
                }
                else
                {
                    key = allocator_->alloc_key(key_size_limit);
                }
                ret.back().key = key;
                inserted_keys_.erase(key);
                break;
            }
            case Op::kGet:
            {
                Key key;
                if (probability_yes(found_rate) && !inserted_keys_.empty())
                {
                    key = random_choose(inserted_keys_, 10000);
                }
                else
                {
                    key = allocator_->alloc_key(key_size_limit);
                }
                ret.back().key = key;
                break;
            }
            case Op::kSync:
            {
                break;
            }
            case Op::kGc:
            {
                break;
            }
            case Op::kSnapshot:
            {
                break;
            }
            case Op::kVisit:
            {
                auto size = inserted_keys_.size();
                if (size == 0)
                {
                    ret.back().op = Op::kNoOp;
                    continue;
                }
                double begin_range = (1 - visit_range_percent);

                double begin_d = fast_pseudo_rand_dbl(0, begin_range);
                double range_percent = visit_range_percent;
                double end_d = begin_d + range_percent;

                size_t begin_idx = size * begin_d;
                size_t end_idx = size * end_d;

                CHECK_GE(end_idx, begin_idx);
                CHECK_LT(end_idx, inserted_keys_.size());

                auto beg_it = inserted_keys_.begin();
                std::advance(beg_it, begin_idx);
                auto end_it = inserted_keys_.begin();
                std::advance(end_it, end_idx);

                if (end_it == inserted_keys_.end())
                {
                    LOG(FATAL) << "Invalid access";
                }

                ret.back().lower = *beg_it;
                ret.back().upper = *end_it;
                break;
            }
            case Op::kNoOp:
            {
                break;
            }
            default:
            {
                LOG(FATAL) << "invalid op " << (int) op;
            }
            }
        }
        return ret;
    }

    // clone and inherit the inserted_keys
    BenchGenerator clone(const std::vector<OpConfig> &op_conf)
    {
        BenchGenerator ret(op_conf, allocator_);
        ret.inserted_keys_ = inserted_keys_;

        return ret;
    }

private:
    std::vector<OpConfig> op_confs_;
    std::map<double, OpConfig> acc_prob_to_conf_;
    std::shared_ptr<IRandAllocator> allocator_;

    std::set<Key> inserted_keys_;
};  // namespace bench

}  // namespace bench
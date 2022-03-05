#pragma once
namespace kvs
{
constexpr static const char *kDefaultTestDir = "./engine_test/";
constexpr static const char *kDefaultBenchDir = "./engine_bench/";
constexpr static size_t kMaxKeySize = 4 * 1024;     // 4KB
constexpr static size_t kMaxValueSize = 16 * 1024;  // 16MB
constexpr static size_t kMaxKeyValueSize = std::max(kMaxKeySize, kMaxValueSize);
}  // namespace kvs
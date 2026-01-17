#include <atomic>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/stat.h>

#include <thread>
#include <immintrin.h>

// AVX2 SIMD newline finder - scans 32 bytes at a time
inline const char* find_newline_avx2(const char* start, const char* end) {
    const __m256i newline = _mm256_set1_epi8('\n');

    // Process 32 bytes at a time
    while (start + 32 <= end) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(start));
        __m256i cmp = _mm256_cmpeq_epi8(chunk, newline);
        int mask = _mm256_movemask_epi8(cmp);
        if (mask != 0) {
            return start + __builtin_ctz(mask);
        }
        start += 32;
    }

    // Handle remaining bytes
    while (start < end) {
        if (*start == '\n') return start;
        start++;
    }
    return nullptr;
}

struct Measure {
  int32_t num_datapoints{};
  double curr_temp{};
  float min{std::numeric_limits<float>::max()};
  float max{std::numeric_limits<float>::min()};
};

struct MeasureV2 {
  int32_t num_datapoints{};
  int64_t sum{0};           // Sum of (temp * 10)
  int16_t min{999};         // temp * 10 (max possible: 99.9 * 10 = 999)
  int16_t max{-999};        // temp * 10 (min possible: -99.9 * 10 = -999)
  bool occupied{false};

  float mean() const {
    return (sum / 10.0) / num_datapoints;
  }
};

template <typename T> void print_map(const T &mp) {
  std::vector<std::pair<std::string, Measure>> vals;
  vals.reserve(mp.size());
  for (auto &[k, v] : mp) {
    vals.push_back(std::make_pair(k, v));
  }
  std::sort(vals.begin(), vals.end(),
            [](auto a, auto b) { return a.first < b.first; });
  std::string ans = "{";
  ans.reserve(1024);
  std::cout << std::fixed;
  for (const auto &val : vals) {
    std::stringstream ss;
    ss << std::fixed;
    float avg = val.second.curr_temp / val.second.num_datapoints;
    ss << std::setprecision(1) << val.first << "=" << avg << "/"
       << val.second.max << "/" << val.second.min << ", ";
    ans += ss.str();
  }
  ans += "}";

  std::cout << ans << std::endl;
}

void naive(const std::string &file_path) {

  std::ifstream fstream;
  fstream.open(file_path.c_str());

  if (!fstream.is_open()) {
    throw std::runtime_error("is not open!");
  }

  std::unordered_map<std::string, Measure> store;

  std::string line;
  while (std::getline(fstream, line)) {

    auto split_range = line | std::views::split(';');
    std::vector<std::string> tokens;
    for (auto &&subrange : split_range) {
      tokens.push_back(std::string(subrange.begin(), subrange.end()));
    }
    std::string const &city = tokens[0];
    float temp = std::stof(tokens[1].c_str());
    Measure &measure = store[city];

    measure.num_datapoints++;
    measure.curr_temp += temp;
    measure.min = std::min(measure.min, temp);
    measure.max = std::max(measure.max, temp);
  }
  print_map(store);
}

long getPageSize_POSIX() {
  // Use _SC_PAGESIZE (or the synonym _SC_PAGE_SIZE)
  long pageSize = sysconf(_SC_PAGESIZE);
  if (pageSize == -1) {
    // Handle error if sysconf fails
    perror("sysconf");
    return -1; // Or some default/error value
  }
  return pageSize;
}

inline std::pair<size_t, uint64_t> fast_hash_with_prefix(const char *buf, size_t len) {
  uint64_t prefix = 0;
  memcpy(&prefix, buf, std::min(len, 8UL));
  uint64_t h = prefix;
  if (len > 8) {
    uint64_t h2 = 0;
    memcpy(&h2, buf + 8, std::min(len - 8, 8UL));
    h ^= h2 << 1;
  }
  h ^= len;
  h *= 0x9e3779b97f4a7c15ULL;
  h ^= h >> 33;
  return {h, prefix};
}

struct HashTable {

  static constexpr size_t HTABLE_SZ = 4096;
  static constexpr size_t HTABLE_MASK = HTABLE_SZ - 1;

  MeasureV2 entries[HTABLE_SZ]{};
  char keys[HTABLE_SZ][32];
  uint8_t key_lens[HTABLE_SZ]{};
  uint64_t key_prefix[HTABLE_SZ]{};  // First 8 bytes for fast comparison

  HashTable() {
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      entries[i].occupied = false;
    }
  }

  inline bool keys_equal(size_t offset, const char *str, size_t len, uint64_t prefix) const {
    if (key_lens[offset] != len) return false;
    if (key_prefix[offset] != prefix) return false;
    if (len <= 8) return true;
    return memcmp(keys[offset] + 8, str + 8, len - 8) == 0;
  }

  MeasureV2 &get(const char *str, size_t len) {
    auto [hash, prefix] = fast_hash_with_prefix(str, len);
    size_t offset = hash & HTABLE_MASK;

    // Fast path: empty slot
    if (!entries[offset].occupied) {
      entries[offset].occupied = true;
      key_lens[offset] = len;
      key_prefix[offset] = prefix;
      memcpy(keys[offset], str, len);
      keys[offset][len] = '\0';
      return entries[offset];
    }

    // Check if it's a match (most common case for repeated cities)
    if (keys_equal(offset, str, len, prefix)) {
      return entries[offset];
    }

    // Linear probing for collision
    offset = (offset + 1) & HTABLE_MASK;
    while (entries[offset].occupied) {
      if (keys_equal(offset, str, len, prefix)) {
        return entries[offset];
      }
      offset = (offset + 1) & HTABLE_MASK;
    }

    // New entry after collision
    entries[offset].occupied = true;
    key_lens[offset] = len;
    key_prefix[offset] = prefix;
    memcpy(keys[offset], str, len);
    keys[offset][len] = '\0';
    return entries[offset];
  }

  void merge(HashTable &other) {
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      if (!other.entries[i].occupied)
        continue;

      // Look up by key, not by index - keys may be at different positions
      MeasureV2 &e1 = get(other.keys[i], other.key_lens[i]);
      auto &e2 = other.entries[i];

      e1.num_datapoints += e2.num_datapoints;
      e1.sum += e2.sum;
      e1.max = std::max(e1.max, e2.max);
      e1.min = std::min(e1.min, e2.min);
    }
  }

  void print() {
    std::vector<std::pair<std::string, MeasureV2>> vals;
    vals.reserve(1024);
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      if (entries[i].occupied) {
        vals.push_back(std::make_pair(keys[i], entries[i]));
      }
    }
    std::sort(vals.begin(), vals.end(),
              [](auto a, auto b) { return a.first < b.first; });
    std::string ans = "{";
    ans.reserve(1024);
    std::cout << std::fixed;
    for (const auto &val : vals) {
      std::stringstream ss;
      ss << std::fixed;
      float avg = val.second.mean();
      float max_val = val.second.max / 10.0f;
      float min_val = val.second.min / 10.0f;
      ss << std::setprecision(1) << val.first << "=" << avg << "/"
         << max_val << "/" << min_val << ", ";
      ans += ss.str();
    }
    ans += "}";

    std::cout << ans << std::endl;
  }
};

void mmap_sol(const std::string &file_path) {

  FILE *fd = fopen(file_path.c_str(), "r");

  /* Get the file size using fstat */
  struct stat file_info;

  if (fstat(fileno(fd), &file_info) == -1) {
    perror("Error getting the file size");
    return;
  }

  size_t bytes = file_info.st_size;

  void *addr = mmap(NULL, bytes, PROT_READ, MAP_SHARED, fileno(fd), 0);

  if (addr == MAP_FAILED) {
    printf("Could not mmap file\n");
    return;
  }

  // Request transparent huge pages for better TLB performance
  madvise(addr, bytes, MADV_HUGEPAGE);

  auto do_chunk = [&](char *chunk_start, char *chunk_end, bool is_first_chunk,
                      HashTable &hash_table) {
    char *byte_arr = (char *)chunk_start;
    size_t curr = 0;

    if (!is_first_chunk) {
      while (*byte_arr != '\n')
        byte_arr++;
      byte_arr++;
    }

    char *curr_line = byte_arr;

    auto process_line = [&]() {
      // Find semicolon by searching backwards - temperature is 3-5 chars
      // Formats: X.X (3), -X.X (4), XX.X (4), -XX.X (5)
      char *line_end = curr_line + curr;
      char *ptr;
      if (line_end[-4] == ';') ptr = line_end - 4;
      else if (line_end[-5] == ';') ptr = line_end - 5;
      else ptr = line_end - 6;

      size_t str_end = ptr - curr_line;
      ptr++;

      // Parse temperature with branch hints
      // Formats: X.X, XX.X, -X.X, -XX.X
      int16_t temp;
      if (__builtin_expect(*ptr != '-', 1)) {
        // Positive (most common case first)
        if (__builtin_expect(ptr[1] == '.', 1)) {
          temp = 10 * (ptr[0] - '0') + (ptr[2] - '0');
        } else {
          temp = 100 * (ptr[0] - '0') + 10 * (ptr[1] - '0') + (ptr[3] - '0');
        }
      } else {
        ptr++;
        if (__builtin_expect(ptr[1] == '.', 1)) {
          temp = -(10 * (ptr[0] - '0') + (ptr[2] - '0'));
        } else {
          temp = -(100 * (ptr[0] - '0') + 10 * (ptr[1] - '0') + (ptr[3] - '0'));
        }
      }

      MeasureV2 &measure = hash_table.get(curr_line, str_end);
      measure.num_datapoints++;
      measure.sum += temp;
      measure.min = std::min(temp, measure.min);
      measure.max = std::max(temp, measure.max);
    };

    while (byte_arr < chunk_end) {
      const char *ptr = find_newline_avx2(byte_arr, chunk_end);
      if (!ptr) return;

      curr = ptr - byte_arr;
      curr_line = byte_arr;
      process_line();
      byte_arr = const_cast<char*>(ptr) + 1;
    }
  };

  size_t n_threads = std::thread::hardware_concurrency();

  std::vector<HashTable> hash_tables;
  std::vector<std::thread> threads;

  hash_tables.resize(n_threads);

  std::atomic_int32_t current_chunk{0};

  const size_t page_size = getPageSize_POSIX();
  size_t chunk_sz = 128 * page_size;
  const size_t n_chunks = (bytes + chunk_sz - 1) / chunk_sz;

  for (size_t i = 0; i < n_threads; ++i) {
    threads.emplace_back([&, i]() {
      while (true) {
        size_t chunk = current_chunk.fetch_add(1, std::memory_order_relaxed);

        if (chunk >= n_chunks)
          break;

        char *start = (char *)addr + chunk * chunk_sz;
        char *end = std::min((char *)addr + (chunk + 1) * chunk_sz,
                             (char *)addr + bytes);

        // Prefetch next chunk while we process this one
        if ((chunk + 1) * chunk_sz < bytes) {
          char *next_start = (char *)addr + (chunk + 1) * chunk_sz;
          for (size_t off = 0; off < chunk_sz; off += 4096) {
            __builtin_prefetch(next_start + off, 0, 0);
          }
        }

        while (end < (char *)addr + bytes && *end != '\n') {
          end++;
        }

        do_chunk(start, end, (chunk == 0), hash_tables[i]);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  for (size_t i = 1; i < n_threads; ++i) {
    hash_tables[0].merge(hash_tables[i]);
  }

  hash_tables[0].print();

  munmap(addr, bytes);
  fclose(fd);
}

int main() {

  // naive("datasets/measurements.txt");
  mmap_sol("datasets/measurements.txt");
}

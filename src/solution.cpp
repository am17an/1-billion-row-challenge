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

struct Measure {
  int32_t num_datapoints{};
  double curr_temp{};
  float min{std::numeric_limits<float>::max()};
  float max{std::numeric_limits<float>::min()};
};

struct FixedPoint {
  int8_t c;
  int8_t mant;

  friend bool operator<(const FixedPoint &a, const FixedPoint &b) {
    if (a.c == b.c) {
      return a.mant < b.mant;
    }
    return a.c < b.c;
  }

  friend std::ostream &operator<<(std::ostream &os, const FixedPoint &x) {
    os << (int)x.c << "." << (int)std::abs(x.mant);
    return os;
  }
};

struct MeasureV2 {
  int32_t num_datapoints{};
  int32_t running_c_sum{0};
  int32_t running_mant_sum{0};
  FixedPoint min{127, 127};
  FixedPoint max{-128, -128};
  bool occupied{false};

  float mean() const {
    double total = running_c_sum + running_mant_sum / 10.;
    return total / num_datapoints;
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

inline size_t fast_hash(const char *buf, size_t len) {
  uint64_t h = 0;
  memcpy(&h, buf, std::min(len, 8UL));
  if (len > 8) {
    uint64_t h2 = 0;
    memcpy(&h2, buf + 8, std::min(len - 8, 8UL));
    h ^= h2 << 1;
  }
  h ^= len;
  h *= 0x9e3779b97f4a7c15ULL;
  h ^= h >> 33;
  return h;
}

struct HashTable {

  static constexpr size_t HTABLE_SZ = 2048;

  MeasureV2 entries[HTABLE_SZ]{};
  char keys[HTABLE_SZ][32];

  size_t occupancy = 0;
  // insert into x, y, z

  HashTable() {
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      entries[i].occupied = false;
    }
  }

  MeasureV2 &get(const char *str, size_t len) {
    size_t hash = fast_hash(str, len);
    size_t offset = hash % HTABLE_SZ;

    if (!entries[offset].occupied) {
      memcpy(keys[offset], str, len);
      keys[offset][len] = '\0';
      entries[offset].occupied = true;
      return entries[offset];
    }

    // Linear probing for collision
    while (entries[offset].occupied && strncmp(keys[offset], str, len) != 0) {
      offset = (offset + 1) % HTABLE_SZ;
    }

    if (!entries[offset].occupied) {
      entries[offset].occupied = true;
      memcpy(keys[offset], str, len);
      keys[offset][len] = '\0';
    }

    return entries[offset];
  }

  void merge(HashTable &other) {
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      if (!other.entries[i].occupied)
        continue;

      // Look up by key, not by index - keys may be at different positions
      size_t key_len = strlen(other.keys[i]);
      MeasureV2 &e1 = get(other.keys[i], key_len);
      auto &e2 = other.entries[i];

      e1.num_datapoints += e2.num_datapoints;
      e1.running_c_sum += e2.running_c_sum;
      e1.running_mant_sum += e2.running_mant_sum;
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
      ss << std::setprecision(1) << val.first << "=" << avg << "/"
         << val.second.max << "/" << val.second.min << ", ";
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

  if (errno != 0) {
    printf("Could not open file\n");
    return;
  }

  // size_t offset = 0;

  // char curr_line[256];

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
      int str_end = 0;

      int32_t exp = 0;
      int32_t mantissa = 0;
      bool negative = false;

      char * ptr = (char *)memchr(curr_line, ';', curr);
      str_end = ptr - curr_line;
      /*
      for (size_t i = 0; i < curr; ++i) {
        if (curr_line[i] == ';') {
          parse_int = true;
          str_end = i;
          break;
        }

        if (parse_int) {
          if (curr_line[i] == '-') {
            negative = true;
            continue;
          }
          if (curr_line[i] == '.') {
            mantissa = curr_line[i + 1] - '0';
            break;
          } else {
            exp = exp * 10 + (curr_line[i] - '0');
          }
        }
      }
      */

      ptr++;
      if (*ptr == '-') {
          negative = true;
          ptr++;
      }

      if (ptr[1] == '.') {
          exp = ptr[0] - '0';
          mantissa = ptr[2] - '0';
      } else if (ptr[2] == '.') {
          exp = (ptr[0] - '0')*10 + (ptr[1] - '0');
          mantissa = ptr[3] - '0';
      } else {
          assert(false);
      }

      MeasureV2 &measure = hash_table.get(curr_line, str_end);

      // float temp = (exp + mantissa/10.0)*(negative ? -1 : 1);
      int8_t c = (negative ? -1 : 1) * exp;
      int8_t mant = (negative ? -1 : 1) * mantissa;
      FixedPoint temp{c, mant};

      measure.num_datapoints++;
      measure.max = std::max(temp, measure.max);
      measure.min = std::min(temp, measure.min);
      measure.running_c_sum += c;
      measure.running_mant_sum += mant;
    };

    while (byte_arr != chunk_end) {
      char * ptr = (char *)memchr(byte_arr, '\n', chunk_end - byte_arr);

      if (!ptr) {
          return;
      }
      curr = ptr - byte_arr;
      curr_line = byte_arr;
      process_line();
      byte_arr = ptr + 1;
    }
  };

  size_t n_threads = std::thread::hardware_concurrency();

  std::vector<HashTable> hash_tables;
  std::vector<std::thread> threads;

  hash_tables.resize(n_threads);

  std::atomic_int32_t current_chunk{0};

  const size_t page_size = getPageSize_POSIX();
  size_t chunk_sz = 64 * page_size;
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

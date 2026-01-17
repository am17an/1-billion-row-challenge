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

struct HashTable {

  static constexpr size_t HTABLE_SZ = 1024;

  MeasureV2 entries[HTABLE_SZ]{};
  char keys[HTABLE_SZ][32];

  size_t occupancy = 0;
  // insert into x, y, z

  HashTable() {
    for (size_t i = 0; i < HTABLE_SZ; ++i) {
      entries[i].occupied = false;
    }
  }

  template <size_t NE> size_t fnv1a_hash(const char *buf) {
    size_t hash = 0xcbf29ce484222325ULL;
#pragma GCC unroll 8
    for (size_t i = 0; i < NE; ++i) {
      hash ^= static_cast<size_t>(buf[i]);
      hash *= 0x100000001b3ULL;
    }
    return hash;
  }

  template <size_t NE> MeasureV2 &hash_str(const char *str) {
    size_t hash = 0;
    char buf[32];
#pragma GCC unroll 8
    for (size_t i = 0; i < NE; ++i) {
      buf[i] = str[i];
      // hash += hash*31 + str[i];
    }

    hash = fnv1a_hash<NE>(str);

    size_t offset = hash % HTABLE_SZ;

    if (!entries[offset].occupied) {
      memcpy(keys[offset], buf, NE);
      keys[offset][NE] = '\0';

      entries[offset].occupied = true;
      return entries[offset];
    } else {
      // printf("Collision! %s %s %zu", keys[hash%HTABLE_SZ], buf, hash);
      int offset = hash % HTABLE_SZ;
      while (entries[offset].occupied && strncmp(keys[offset], buf, NE) != 0) {
        offset = (offset + 1) % HTABLE_SZ;
      }

      if (!entries[offset].occupied) {
        entries[offset].occupied = true;
        memcpy(keys[offset], buf, NE);
        keys[offset][NE] = '\0';
      }

      return entries[offset];
    }
  }
  MeasureV2 &get(const char *begin, size_t sz) {

#define CASE(N)                                                                \
  case N:                                                                      \
    return hash_str<N>(begin);                                                 \
    break;
    switch (sz) {
      CASE(1);
      CASE(2);
      CASE(3);
      CASE(4);
      CASE(5);
      CASE(6);
      CASE(7);
      CASE(8);
      CASE(9);
      CASE(10);
      CASE(11);
      CASE(12);
      CASE(13);
      CASE(14);
      CASE(15);
      CASE(16);
      CASE(17);
    default:
      assert(false);
      break;
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

  const size_t page_size = getPageSize_POSIX();

  size_t offset = 0;

  char *byte_arr = (char *)addr;
  // char curr_line[256];
  char *curr_line = byte_arr;
  size_t curr = 0;

  // std::unordered_map<std::string, Measure> mp;
  // mp.reserve(500);

  HashTable hash_table;

  auto process_line = [&]() {
    int str_end = 0;
    bool parse_int = false;

    int32_t exp = 0;
    int32_t mantissa = 0;
    bool negative = false;

    for (size_t i = 0; i < curr; ++i) {
      if (curr_line[i] == ';') {
        parse_int = true;
        str_end = i;
        continue;
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

  while (offset < bytes) {
    for (size_t i = 0; i < page_size && i + offset < bytes; ++i) {
      if (byte_arr[i] == '\n') {
        process_line();
        curr = 0;
        curr_line = &byte_arr[i + 1];
      } else {
        // curr_line[curr++] = byte_arr[i];
        curr++;
      }
    }

    offset += page_size;
    byte_arr += page_size;
  }

  // print_map(mp);
  hash_table.print();

  munmap(addr, bytes);
}

int main() {

  // naive("datasets/measurements.txt");
  mmap_sol("datasets/measurements.txt");
}

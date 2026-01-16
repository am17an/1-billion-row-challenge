#include <cmath>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <sstream>
#include <string_view>
#include <unordered_map>

#include <vector>
#include <algorithm>
#include <ranges>
#include <cstdio>

#include <unistd.h>    
#include <sys/mman.h>

#include <sys/stat.h>
#include <fcntl.h>

struct Measure {
    int32_t num_datapoints{};
    double  curr_temp{};
    float   min{std::numeric_limits<float>::max()};
    float   max{std::numeric_limits<float>::min()};
};

struct FNV1aHash {
    size_t operator()(const std::string& s) const {
        size_t hash = 14695981039346656037ULL;
        for (char c : s) {
            hash ^= static_cast<size_t>(c);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};

template<typename T>
void print_map(const T & mp) {
    std::vector<std::pair<std::string, Measure>> vals;
    vals.reserve(mp.size());
    for (auto & [k, v]: mp) {
       vals.push_back(std::make_pair(k, v)); 
    }
    std::sort(vals.begin(), vals.end(), [](auto a, auto b){
        return a.first < b.first;
    });
    std::string ans = "{";
    ans.reserve(1024);
    std::cout << std::fixed;
    for (const auto & val: vals) {
        std::stringstream ss;
        ss << std::fixed;
        float avg = val.second.curr_temp/val.second.num_datapoints;
        ss << std::setprecision(1) << val.first << "=" << avg << "/"
            << val.second.max << "/" << val.second.min << ", ";
        ans += ss.str(); 
    }
    ans += "}";

    std::cout << ans << std::endl;
}

void naive(const std::string & file_path) {

    std::ifstream fstream;
    fstream.open(file_path.c_str());

    if(!fstream.is_open()) {
        throw std::runtime_error("is not open!");
    }

    std::unordered_map<std::string, Measure> store;

    std::string line;
    while(std::getline(fstream, line)) {

        auto split_range = line | std::views::split(';'); 
        std::vector<std::string> tokens;
        for (auto && subrange: split_range) {
            tokens.push_back(std::string(subrange.begin(), subrange.end()));
        }
        std::string const & city = tokens[0];
        float        temp = std::stof(tokens[1].c_str());
        Measure & measure = store[city];

        measure.num_datapoints++ ;
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

void mmap_sol(const std::string & file_path) {

    FILE * fd = fopen(file_path.c_str(), "r");

    /* Get the file size using fstat */
    struct stat file_info;

    if (fstat(fileno(fd), &file_info) == -1) {
        perror("Error getting the file size");
        return;
    }

    size_t bytes = file_info.st_size;

    void * addr = mmap(NULL, bytes, PROT_READ, MAP_SHARED, fileno(fd), 0);

    if (errno != 0) {
        std::cout << strerror(errno) << std::endl;
        return;
    }

    const size_t page_size = getPageSize_POSIX();

    size_t offset = 0;

    char * byte_arr = (char *)addr;
    char curr_line[256];
    size_t curr = 0;

    std::unordered_map<std::string, Measure, FNV1aHash> mp;
    mp.reserve(500);

    auto process_line = [&]() {
        int str_end = 0;
        bool parse_int = false;

        int32_t exp = 0;
        int32_t mantissa = 0; 
        bool negative = false;

        for (int i = 0 ; i < curr; ++i) {
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
                    mantissa = curr_line[i+1] - '0';
                    break;
                } else {
                    exp = exp*10 + (curr_line[i] - '0');
                }
            }
        }

        Measure & measure = mp[std::string(curr_line, curr_line + str_end)];

        float temp = (exp + mantissa/10.0)*(negative ? -1 : 1);

        measure.num_datapoints++;
        measure.max = std::max(temp, measure.max);
        measure.min = std::min(temp, measure.min);
        measure.curr_temp += temp;
    };

    while (offset < bytes) {
        for (int i = 0; i < page_size && i + offset < bytes; ++i) {
            if (byte_arr[i] == '\n') {
                process_line();
                curr = 0;
            } else {
                curr_line[curr++] = byte_arr[i];
            }
        }

        offset += page_size;
        byte_arr += page_size;
    }

    print_map(mp);

    munmap(addr, bytes);
}

int main() {

    //naive("datasets/measurements.txt");
    mmap_sol("datasets/measurements.txt");
}
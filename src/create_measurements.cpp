#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

struct Station {
  std::string name;
  float mean_temp;
};

std::vector<Station> load_stations(const std::string &filepath) {
  std::vector<Station> stations;
  std::ifstream file(filepath);

  if (!file.is_open()) {
    std::cerr << "Error: Could not open " << filepath << std::endl;
    exit(1);
  }

  std::string line;
  std::getline(file, line); // Skip header

  while (std::getline(file, line)) {
    size_t comma = line.find(',');
    if (comma != std::string::npos) {
      Station s;
      s.name = line.substr(0, comma);
      s.mean_temp = std::stof(line.substr(comma + 1));
      stations.push_back(s);
    }
  }

  return stations;
}

void generate_chunk(const std::vector<Station> &stations, size_t num_rows,
                    std::string &output, unsigned int seed) {
  std::mt19937 rng(seed);
  std::uniform_int_distribution<size_t> station_dist(0, stations.size() - 1);

  output.reserve(num_rows * 20); // Estimate ~20 bytes per line

  char buf[64];

  for (size_t i = 0; i < num_rows; ++i) {
    const Station &s = stations[station_dist(rng)];

    // Normal distribution around mean temp with stddev of 10
    std::normal_distribution<float> temp_dist(s.mean_temp, 10.0f);
    float temp = temp_dist(rng);

    // Clamp to valid range
    temp = std::max(-99.9f, std::min(99.9f, temp));

    // Format with 1 decimal place
    int len = snprintf(buf, sizeof(buf), "%s;%.1f\n", s.name.c_str(), temp);
    output.append(buf, len);
  }
}

int main(int argc, char *argv[]) {
  size_t num_rows = 1'000'000'000; // Default 1 billion

  if (argc > 1) {
    num_rows = std::stoull(argv[1]);
  }

  const std::string stations_file = "data/weather_stations.csv";
  const std::string output_file = "datasets/measurements.txt";

  std::cout << "Loading weather stations..." << std::endl;
  std::vector<Station> stations = load_stations(stations_file);
  std::cout << "Loaded " << stations.size() << " stations" << std::endl;

  size_t n_threads = std::thread::hardware_concurrency();
  std::cout << "Using " << n_threads << " threads" << std::endl;
  std::cout << "Generating " << num_rows << " measurements..." << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();

  // Each thread generates its chunk into a string buffer
  std::vector<std::string> buffers(n_threads);
  std::vector<std::thread> threads;

  size_t rows_per_thread = num_rows / n_threads;
  size_t remainder = num_rows % n_threads;

  for (size_t i = 0; i < n_threads; ++i) {
    size_t thread_rows = rows_per_thread + (i < remainder ? 1 : 0);
    threads.emplace_back(generate_chunk, std::cref(stations), thread_rows,
                         std::ref(buffers[i]), (unsigned int)(i + 42));
  }

  // Wait for all threads
  for (auto &t : threads) {
    t.join();
  }

  auto gen_time = std::chrono::high_resolution_clock::now();
  auto gen_elapsed =
      std::chrono::duration<double>(gen_time - start_time).count();
  std::cout << "Generation completed in " << gen_elapsed << "s" << std::endl;

  // Write all buffers to file
  std::cout << "Writing to file..." << std::endl;
  FILE *f = fopen(output_file.c_str(), "w");
  if (!f) {
    std::cerr << "Error: Could not open output file" << std::endl;
    return 1;
  }

  for (size_t i = 0; i < n_threads; ++i) {
    fwrite(buffers[i].data(), 1, buffers[i].size(), f);
    buffers[i].clear();
    buffers[i].shrink_to_fit(); // Free memory as we go
  }
  fclose(f);

  auto end_time = std::chrono::high_resolution_clock::now();
  auto total_elapsed =
      std::chrono::duration<double>(end_time - start_time).count();

  std::cout << "Completed in " << total_elapsed << "s" << std::endl;
  std::cout << "Rate: " << (num_rows / total_elapsed / 1e6)
            << " million rows/sec" << std::endl;
  std::cout << "Output: " << output_file << std::endl;

  return 0;
}

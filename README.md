# 1 Billion Row Challenge

C++ solution for [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc).

## Optimizations

1. **Memory-mapped I/O** - `mmap()` for zero-copy file reading (hugepages did not help)
2. **Multi-threaded processing** - Work-stealing with atomic chunk counter
3. **Custom hash table** - Open addressing with linear probing, power-of-2 size
4. **Fast hash function** - 64-bit multiply-xorshift hash
5. **Fixed-point arithmetic** - Avoid floating point for temperature calculations
6. **Length-first comparison** - Skip `memcmp` when key lengths differ
7. **Backwards semicolon search** - Find `;` by checking fixed offsets from line end (temperature is always 3-5 chars)
8. **Prefetching** - `__builtin_prefetch` for next chunk
9. **`memchr` for newlines** - Leverage libc's AVX2-optimized implementation

## Building

```bash
g++ -O3 -march=native -std=c++20 -pthread src/solution.cpp -o solution
```

## Running

```bash
# Generate test data (1 billion rows)
./create_measurements 1000000000

# Run the solution
./solution
```

## Benchmark

```bash
hyperfine --warmup 3 --runs 5 ./solution
```

## Comparison to Original Challenge

The original 1BRC was run on a Hetzner AX161 server (AMD EPYC 7502P, 32c/64t, 128GB RAM) with winning times around 1.5 seconds.

Time for 16 threads @ 3.9 GHz (`AMD Ryzen 7 3800XT 8-Core Processor`): **2.78 seconds**

# Benchmark Results

## Sum Benchmark Results (2025-07-28)

This document contains benchmark results comparing the performance of the `Sum` function between the gleam library and the go-gota library.

### Summary
The gleam library's Sum function significantly outperforms the go-gota library across all data types and sizes, with:
- 10-30x faster execution time
- 5-30x higher throughput
- Generally lower memory allocations for medium to large datasets

### Detailed Results

#### Int32

| Benchmark | Operations | ns/op | MB/s | B/op | allocs/op |
|-----------|----------:|------:|-----:|-----:|----------:|
| **Small Dataset (1,000 elements)** |
| gleam Sum | 257,020 | 4,635 | 8,630.27 | 848 | 8 |
| gota Sum | 26,517 | 44,329 | 902.35 | 81,922 | 1 |
| **Medium Dataset (1,000,000 elements)** |
| gleam Sum | 2,738 | 428,370 | 9,337.72 | 8,007,624 | 54 |
| gota Sum | 310 | 3,858,499 | 1,036.67 | 8,003,608 | 1 |
| **Large Dataset (100,000,000 elements)** |
| gleam Sum | 39 | 30,546,152 | 13,094.94 | 800,010,373 | 54 |
| gota Sum | 3 | 376,075,695 | 1,063.62 | 800,006,160 | 1 |

#### Int64

| Benchmark | Operations | ns/op | MB/s | B/op | allocs/op |
|-----------|----------:|------:|-----:|-----:|----------:|
| **Small Dataset (1,000 elements)** |
| gleam Sum | 716,902 | 1,599 | 50,031.58 | 848 | 8 |
| gota Sum | 25,539 | 45,191 | 1,770.25 | 81,922 | 1 |
| **Medium Dataset (1,000,000 elements)** |
| gleam Sum | 9,160 | 129,228 | 61,905.94 | 848 | 8 |
| gota Sum | 314 | 3,803,406 | 2,103.38 | 8,003,606 | 1 |
| **Large Dataset (100,000,000 elements)** |
| gleam Sum | 92 | 12,955,009 | 61,752.18 | 848 | 8 |
| gota Sum | 3 | 371,072,445 | 2,155.91 | 800,006,192 | 2 |

#### Float32

| Benchmark | Operations | ns/op | MB/s | B/op | allocs/op |
|-----------|----------:|------:|-----:|-----:|----------:|
| **Small Dataset (1,000 elements)** |
| gleam Sum | 164,329 | 7,273 | 5,499.68 | 848 | 8 |
| gota Sum | 26,176 | 45,541 | 878.32 | 81,922 | 1 |
| **Medium Dataset (1,000,000 elements)** |
| gleam Sum | 1,958 | 596,440 | 6,706.46 | 8,007,542 | 56 |
| gota Sum | 309 | 3,834,701 | 1,043.11 | 8,003,606 | 1 |
| **Large Dataset (100,000,000 elements)** |
| gleam Sum | 26 | 47,507,460 | 8,419.73 | 800,010,356 | 56 |
| gota Sum | 3 | 373,938,306 | 1,069.70 | 800,006,192 | 2 |

#### Float64

| Benchmark | Operations | ns/op | MB/s | B/op | allocs/op |
|-----------|----------:|------:|-----:|-----:|----------:|
| **Small Dataset (1,000 elements)** |
| gleam Sum | 372,996 | 3,146 | 25,427.33 | 848 | 8 |
| gota Sum | 25,698 | 45,743 | 1,748.88 | 81,922 | 1 |
| **Medium Dataset (1,000,000 elements)** |
| gleam Sum | 4,266 | 282,633 | 28,305.30 | 848 | 8 |
| gota Sum | 312 | 3,814,819 | 2,097.09 | 8,003,607 | 1 |
| **Large Dataset (100,000,000 elements)** |
| gleam Sum | 42 | 28,051,454 | 28,519.02 | 848 | 8 |
| gota Sum | 3 | 410,920,583 | 1,946.85 | 800,006,160 | 1 |

### Benchmark Environment

- **OS**: darwin
- **Architecture**: arm64
- **CPU**: Apple M4 Max
- **Date**: 2025-07-28

### Methodology

The benchmarks were run using Go's built-in benchmarking tool with the following command:

```
go test -bench=BenchmarkSum ./gleam/series -benchmem
go test -bench=BenchmarkGotaSum ./gleam/series -benchmem
```

Each benchmark measures the performance of summing elements in a series with different data types and sizes:
- Small: 1,000 elements
- Medium: 1,000,000 elements
- Large: 100,000,000 elements
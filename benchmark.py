import time
from bitcask import Bitcask

# Initialize database
db = Bitcask()

# Number of operations
N = 10000

# Benchmark writes
print(f"Benchmarking {N} writes...")
start = time.time()
for i in range(N):
    db.put(f"key{i}".encode(), b"value")
end = time.time()

write_time = end - start
print(f"Writes: {write_time:.2f}s ({N/write_time:.0f} ops/sec)")

# Benchmark reads
print(f"\nBenchmarking {N} reads...")
start = time.time()
for i in range(N):
    db.get(f"key{i}".encode())
end = time.time()

read_time = end - start
print(f"Reads: {read_time:.2f}s ({N/read_time:.0f} ops/sec)")

# Benchmark compaction
print(f"\nBenchmarking compaction...")
start = time.time()
db.compact()
end = time.time()

compact_time = end - start
print(f"Compaction: {compact_time:.2f}s")

db.close()
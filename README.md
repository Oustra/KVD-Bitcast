# Bitcask Key-Value Store (Python)

This project implements a minimal working key/value (KV) store in Python, inspired by the 
design described in Designing Data-Intensive Applications. 
Bitcask is a log-structured key/value storage engine that focuses on high write throughput, 
simple indexing, and efficient reads. It stores all writes as append-only records in files on disk 
while keeping an in-memory hash table mapping keys to file offsets.  
This allows reads to require only a single disk seek, while sequential writes remain extremely 
fast. We implemented Bitcask from scratch in Python, creating our own file format, in-memory 
index, and compaction logic. We chose Bitcask because its design is simple, yet it illustrates the 
core mechanisms of KV stores fast ingestion, efficient lookups, crash recovery, and compaction 
making it an ideal model for a learning project. 

The goals of this project are to explore: 
- How to optimally lay out KV-store data on disk 
- Fast ingestion using append-only logs 
- Efficient key-based lookups 
- Crash recovery and compaction

---

## Project Structure
- bitcask.py          : Bitcask implementation 
- benchmark.py        : Lightweight benchmarking script  
- data/               : Data and hint files (auto-created at runtime)  
- requirements.txt    : Python dependencies (for benchmarking/testing)  
- README.md           : Documentation  

---

## How Bitcask Works (Simple Explanation)
Bitcask is based on 4 simple mechanisms :

### 1. Append-Only Writes
Every write is appended to the end of a log file.  
No random overwrites → fast sequential I/O.

### 2. In-Memory Hash Index
We store:
key → (file_path, offset, record_length)
This gives O(1) reads (lookup in RAM, then one seek on disk).

### 3. Startup Recovery
On initialization, Bitcask scans existing data files and rebuilds the index by reading records in file order. The last record for a key wins.

### 4. Compaction 
Because old versions remain in the log, compaction rewrites only the newest value for each key into a new clean file and deletes old files.

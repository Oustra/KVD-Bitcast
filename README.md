# Bitcask Key-Value Store (Python)

This project is a minimal working implementation of a Bitcask-style
key/value store in Python.  
The goal is to demonstrate how log-structured KV stores work internally:
- Append-only data files
- In-memory hash index
- Simple recovery from data files
- Fast writes & reads
- Optional compaction to clean old data

---

## Project Structure
- bitcask.py          : Bitcask implementation 
- benchmark.py        : Lightweight benchmarking script  
- data/               : Data and hint files (auto-created at runtime)  
- requirements.txt    : Python dependencies (for benchmarking/testing)  
- README.md           : Documentation  

---

## How Bitcask Works (Simple Explanation)
Bitcask is based on 4 simple mechanisms:

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
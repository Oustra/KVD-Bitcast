import os
import struct
import time

class Bitcask:
    """
    Bitcask Implementation:
    - append-only log
    - in-memory key directory
    - simple compaction
    """
    RECORD_HEADER = struct.Struct("!QII")  
    # timestamp (8 bytes), key_size (4), value_size (4)

    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.active_file = None
        self.active_path = None
        self.index = {}  # key -> (file_path, offset, record_length)

        self._open_active_file()
        self._load_existing_files()

    # -----------------------------------------------------------
    # Startup logic
    # -----------------------------------------------------------

    def _open_active_file(self):
        """Open or create the latest data file."""
        files = [f for f in os.listdir(self.data_dir) if f.endswith(".data")]
        if not files:
            fname = "0001.data"
        else:
            fname = sorted(files)[-1]

        self.active_path = os.path.join(self.data_dir, fname)
        self.active_file = open(self.active_path, "ab+")

    def _load_existing_files(self):
        """Scan all .data files to rebuild the in-memory index."""
        for filename in sorted(os.listdir(self.data_dir)):
            if not filename.endswith(".data"):
                continue
            path = os.path.join(self.data_dir, filename)
            self._rebuild_index_from_file(path)

    def _rebuild_index_from_file(self, path):
        """Rebuild index with partial record handling."""
        with open(path, "rb") as f:
            offset = 0
            while True:
                header = f.read(self.RECORD_HEADER.size)
                if not header:
                    break
    
                if len(header) < self.RECORD_HEADER.size:
                    break

                timestamp, ksz, vsz = self.RECORD_HEADER.unpack(header)
                key = f.read(ksz)
                value = f.read(vsz)
                
                if len(key) != ksz or len(value) != vsz:
                    break

                record_length = self.RECORD_HEADER.size + ksz + vsz

                if vsz == 0:
                    if key in self.index:
                        del self.index[key]
                else:
                    self.index[key] = (path, offset, record_length)

                offset += record_length

    # -----------------------------------------------------------
    # Core API
    # -----------------------------------------------------------

    def put(self, key: bytes, value: bytes):
        """Write a key/value pair."""
        if not isinstance(key, bytes) or not isinstance(value, bytes):
            raise TypeError("key and value must be bytes")

        timestamp = int(time.time())
        record = self.RECORD_HEADER.pack(timestamp, len(key), len(value)) + key + value

        offset = self.active_file.tell()
        self.active_file.write(record)
        self.active_file.flush()

        self.index[key] = (self.active_path, offset, len(record))

    def get(self, key: bytes):
        """Retrieve a value by key."""
        if key not in self.index:
            return None

        path, offset, length = self.index[key]

        try:
            with open(path, "rb") as f:
                f.seek(offset)
                header = f.read(self.RECORD_HEADER.size)
                timestamp, ksz, vsz = self.RECORD_HEADER.unpack(header)
                k = f.read(ksz)
                v = f.read(vsz)
            return v
        except (IOError, OSError):
            del self.index[key]
            return None

    def delete(self, key: bytes):
        """Write a tombstone record."""
        if key not in self.index:
            return

        timestamp = int(time.time())
        tombstone = self.RECORD_HEADER.pack(timestamp, len(key), 0) + key

        self.active_file.write(tombstone)
        self.active_file.flush()

        del self.index[key]

    # -----------------------------------------------------------
    # Simple compaction
    # -----------------------------------------------------------

    def compact(self):
        """Merge all keys into a new clean file."""
        self.active_file.close()
        temp_path = os.path.join(self.data_dir, "compact.tmp")
        
        # Write all live keys to temp file
        with open(temp_path, "wb") as newf:
            offset = 0
            for key, (old_path, old_offset, old_length) in list(self.index.items()):
                # Read value directly from old file
                with open(old_path, "rb") as f:
                    f.seek(old_offset)
                    header = f.read(self.RECORD_HEADER.size)
                    timestamp, ksz, vsz = self.RECORD_HEADER.unpack(header)
                    f.read(ksz) 
                    val = f.read(vsz)

                # Write into new compacted file
                timestamp = int(time.time())
                record = self.RECORD_HEADER.pack(timestamp, len(key), len(val)) + key + val
                newf.write(record)
                self.index[key] = (temp_path, offset, len(record))
                offset += len(record)

        # FIX: Rename temp file BEFORE deleting old files
        final_path = os.path.join(self.data_dir, "0001.data")
        
        # Delete old files safely
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".data"):
                os.remove(os.path.join(self.data_dir, filename))
        
        # Now rename temp to final
        os.rename(temp_path, final_path)
        
        # Update index to point to new file
        for key in list(self.index.keys()):
            path, offset, length = self.index[key]
            self.index[key] = (final_path, offset, length)

        # Reopen active file for new writes
        self._open_active_file()

    # -----------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------
    
    def close(self):
        """Close the database properly."""
        if self.active_file:
            self.active_file.close()


# -----------------------------------------------------------
# Usage example
# -----------------------------------------------------------
if __name__ == "__main__":
    db = Bitcask()

    db.put(b"name", b"Alice")
    print("name:", db.get(b"name"))

    db.put(b"name", b"Bob")
    print("updated name:", db.get(b"name"))

    db.delete(b"name")
    print("after delete:", db.get(b"name"))

    db.put(b"x", b"1")
    db.put(b"y", b"2")
    db.compact()
    print("x after compact:", db.get(b"x"))
    
    # Clean shutdown
    db.close()
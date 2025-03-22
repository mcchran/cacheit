from cache import CacheStore, CachePipeline
from typing import Any, List, Optional
import time

class MemoryStore(CacheStore):
    """In-memory implementation of the CacheStore interface (for testing)."""

    def __init__(self):
        """Initialize in-memory storage structures."""
        self.data = {}  # For regular key-value pairs
        self.lists = {}  # For list operations
        self.expiry = {}  # For TTL tracking
        self.counters = {}  # For incr/decr operations

    def _check_expiry(self, key: str) -> bool:
        """Check if a key has expired and remove it if so."""
        if key in self.expiry and time.time() > self.expiry[key]:
            if key in self.data:
                del self.data[key]
            if key in self.expiry:
                del self.expiry[key]
            return True
        return False

    def get(self, key: str) -> Optional[bytes]:
        if self._check_expiry(key):
            return None
        # print("key has expired! Damn!")
        return self.data.get(key)

    def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        self.data[key] = value
        if ttl is not None:
            self.expiry[key] = time.time() + ttl
        elif key in self.expiry:
            del self.expiry[key]
        return True

    def delete(self, key: str) -> bool:
        if key in self.data:
            del self.data[key]
            if key in self.expiry:
                del self.expiry[key]
            return True
        return False

    def exists(self, key: str) -> bool:
        if self._check_expiry(key):
            return False
        return key in self.data

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        if key not in self.lists:
            return []
        if end == -1:
            end = len(self.lists[key])
        return self.lists.get(key, [])[start : end + 1]

    def lindex(self, key: str, index: int) -> Optional[str]:
        if key not in self.lists:
            return None
        try:
            return self.lists[key][index]
        except IndexError:
            return None

    def lrem(self, key: str, count: int, value: str) -> int:
        if key not in self.lists:
            return 0

        removed = 0
        if count > 0:
            # Remove first 'count' occurrences
            for i in range(count):
                try:
                    self.lists[key].remove(value)
                    removed += 1
                except ValueError:
                    break
        elif count < 0:
            # Remove last 'count' occurrences
            for i in range(abs(count)):
                try:
                    # Find from the end
                    idx = len(self.lists[key]) - 1 - self.lists[key][::-1].index(value)
                    del self.lists[key][idx]
                    removed += 1
                except ValueError:
                    break
        else:
            # Remove all occurrences
            original_len = len(self.lists[key])
            self.lists[key] = [x for x in self.lists[key] if x != value]
            removed = original_len - len(self.lists[key])

        return removed

    def rpush(self, key: str, *values: str) -> int:
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].extend(values)
        return len(self.lists[key])

    def incr(self, key: str) -> int:
        if key not in self.counters:
            self.counters[key] = 0
        self.counters[key] += 1
        return self.counters[key]

    def decr(self, key: str) -> int:
        if key not in self.counters:
            self.counters[key] = 0
        self.counters[key] -= 1
        return self.counters[key]

    def pipeline(self) -> "CachePipeline":
        return MemoryPipeline(self)


class MemoryPipeline(CachePipeline):
    """In-memory implementation of the CachePipeline interface."""

    def __init__(self, store: MemoryStore):
        self.store = store
        self.operations = []

    def get(self, key: str):
        self.operations.append(("get", key))
        return self

    def set(self, key: str, value: bytes, ttl: Optional[int] = None):
        self.operations.append(("set", key, value, ttl))
        return self

    def setex(self, key: str, ttl: int, value: bytes):
        self.operations.append(("set", key, value, ttl))
        return self

    def delete(self, key: str):
        self.operations.append(("delete", key))
        return self

    def lrem(self, key: str, count: int, value: str):
        self.operations.append(("lrem", key, count, value))
        return self

    def rpush(self, key: str, *values: str):
        self.operations.append(("rpush", key, values))
        return self

    def incr(self, key: str):
        self.operations.append(("incr", key))
        return self

    def decr(self, key: str):
        self.operations.append(("decr", key))
        return self

    def execute(self) -> List[Any]:
        results = []
        for op in self.operations:
            op_name = op[0]
            if op_name == "get":
                results.append(self.store.get(op[1]))
            elif op_name == "set":
                _, key, value, ttl = op
                results.append(self.store.set(key, value, ttl))
            elif op_name == "delete":
                results.append(self.store.delete(op[1]))
            elif op_name == "lrem":
                _, key, count, value = op
                results.append(self.store.lrem(key, count, value))
            elif op_name == "rpush":
                _, key, values = op
                results.append(self.store.rpush(key, *values))
            elif op_name == "incr":
                results.append(self.store.incr(op[1]))
            elif op_name == "decr":
                results.append(self.store.decr(op[1]))

        self.operations = []
        return results


class CleanMemoryStore(MemoryStore):
    def __init__(self, cleanup_interval=60):
        """Initialize in-memory storage with automatic cleanup."""
        self.data = {}
        self.lists = {}
        self.expiry = {}
        self.counters = {}
        self.cleanup_interval = cleanup_interval
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        """Start a background thread to clean up expired items."""
        import threading

        def cleanup_task():
            while True:
                print("Cleaning up expired keys...")
                time.sleep(self.cleanup_interval)
                self.cleanup_expired()

        self._cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        self._cleanup_thread.start()

    def cleanup_expired(self):
        """Remove all expired keys from memory."""
        now = time.time()
        expired_keys = [
            key for key, expire_time in list(self.expiry.items()) if now > expire_time
        ]

        for key in expired_keys:
            self.delete(key)

        return len(expired_keys)
import pickle
import time
import threading
import multiprocessing
import multiprocessing.shared_memory
from multiprocessing.managers import BaseManager
from typing import Any, Dict, List, Optional

from cache import CacheStore, CachePipeline  # Import your base classes


class SharedObject:
    """A simple wrapper for objects to be shared via Manager."""
    def __init__(self):
        self.data = {}
        self.lists = {}
        self.expiry = {}
        self.counters = {}
    # FIXME: the properties cannot be proxied ... we need mothods to support that 
    # over the automproxy created by the BaseManger registration ... 

class SharedMemoryStore(CacheStore):
    """
    An implementation of CacheStore that uses multiprocessing.Manager 
    to share data across multiple processes on the same machine.
    
    Note: This is for testing only and not suitable for production use
    across multiple machines.
    """
    
    def __init__(self, cleanup_interval=60):
        """Initialize shared memory storage with automatic cleanup."""
        # Create a multiprocessing manager to share objects between processes
        BaseManager.register('SharedObject', SharedObject)
        self.manager = BaseManager()
        self.manager.start()
        
        # Create shared objects
        self.shared = self.manager.SharedObject()
        
        # Set up cleanup
        self.cleanup_interval = cleanup_interval
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start a background thread to clean up expired items."""
        def cleanup_task():
            while True:
                time.sleep(self.cleanup_interval)
                self.cleanup_expired()
        
        self._cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        self._cleanup_thread.start()
    
    def cleanup_expired(self):
        """Remove all expired keys from memory."""
        now = time.time()
        expired_keys = []
        
        # Find expired keys
        for key, expire_time in list(self.shared.expiry.items()):
            if now > expire_time:
                expired_keys.append(key)
        
        # Delete expired keys
        for key in expired_keys:
            self.delete(key)
        
        return len(expired_keys)
    
    def _check_expiry(self, key: str) -> bool:
        """Check if a key has expired and remove it if so."""
        import ipdb; ipdb.set_trace()
        if key in self.shared.expiry and time.time() > self.shared.expiry[key]:
            if key in self.shared.data:
                del self.shared.data[key]
            if key in self.shared.expiry:
                del self.shared.expiry[key]
            return True
        return False
    
    def get(self, key: str) -> Optional[bytes]:
        if self._check_expiry(key):
            return None
        return self.shared.data.get(key)
    
    def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        self.shared.data[key] = value
        if ttl is not None:
            self.shared.expiry[key] = time.time() + ttl
        elif key in self.shared.expiry:
            del self.shared.expiry[key]
        return True
    
    def delete(self, key: str) -> bool:
        if key in self.shared.data:
            del self.shared.data[key]
            if key in self.shared.expiry:
                del self.shared.expiry[key]
            return True
        return False
    
    def exists(self, key: str) -> bool:
        if self._check_expiry(key):
            return False
        return key in self.shared.data
    
    def lrange(self, key: str, start: int, end: int) -> List[str]:
        if key not in self.shared.lists:
            return []
        if end == -1:
            end = len(self.shared.lists[key])
        return self.shared.lists.get(key, [])[start:end+1]
    
    def lindex(self, key: str, index: int) -> Optional[str]:
        if key not in self.shared.lists:
            return None
        try:
            return self.shared.lists[key][index]
        except IndexError:
            return None
    
    def lrem(self, key: str, count: int, value: str) -> int:
        if key not in self.shared.lists:
            return 0
        
        removed = 0
        if count > 0:
            # Remove first 'count' occurrences
            for i in range(count):
                try:
                    self.shared.lists[key].remove(value)
                    removed += 1
                except ValueError:
                    break
        elif count < 0:
            # Remove last 'count' occurrences
            for i in range(abs(count)):
                try:
                    # Find from the end
                    idx = len(self.shared.lists[key]) - 1 - self.shared.lists[key][::-1].index(value)
                    del self.shared.lists[key][idx]
                    removed += 1
                except ValueError:
                    break
        else:
            # Remove all occurrences
            original_len = len(self.shared.lists[key])
            self.shared.lists[key] = [x for x in self.shared.lists[key] if x != value]
            removed = original_len - len(self.shared.lists[key])
        
        return removed
    
    def rpush(self, key: str, *values: str) -> int:
        if key not in self.shared.lists:
            self.shared.lists[key] = []
        self.shared.lists[key].extend(values)
        return len(self.shared.lists[key])
    
    def incr(self, key: str) -> int:
        if key not in self.shared.counters:
            self.shared.counters[key] = 0
        self.shared.counters[key] += 1
        return self.shared.counters[key]
    
    def decr(self, key: str) -> int:
        if key not in self.shared.counters:
            self.shared.counters[key] = 0
        self.shared.counters[key] -= 1
        return self.shared.counters[key]
    
    def pipeline(self) -> 'CachePipeline':
        return SharedMemoryPipeline(self)


class SharedMemoryPipeline(CachePipeline):
    """Pipeline implementation for SharedMemoryStore."""
    
    def __init__(self, store: SharedMemoryStore):
        self.store = store
        self.operations = []
    
    def get(self, key: str):
        self.operations.append(('get', key))
        return self
    
    def set(self, key: str, value: bytes, ttl: Optional[int] = None):
        self.operations.append(('set', key, value, ttl))
        return self
    
    def setex(self, key: str, ttl: int, value: bytes):
        self.operations.append(('set', key, value, ttl))
        return self
    
    def delete(self, key: str):
        self.operations.append(('delete', key))
        return self
    
    def lrem(self, key: str, count: int, value: str):
        self.operations.append(('lrem', key, count, value))
        return self
    
    def rpush(self, key: str, *values: str):
        self.operations.append(('rpush', key, values))
        return self
    
    def incr(self, key: str):
        self.operations.append(('incr', key))
        return self
    
    def decr(self, key: str):
        self.operations.append(('decr', key))
        return self
    
    def execute(self) -> List[Any]:
        results = []
        for op in self.operations:
            op_name = op[0]
            if op_name == 'get':
                results.append(self.store.get(op[1]))
            elif op_name == 'set':
                _, key, value, ttl = op
                results.append(self.store.set(key, value, ttl))
            elif op_name == 'delete':
                results.append(self.store.delete(op[1]))
            elif op_name == 'lrem':
                _, key, count, value = op
                results.append(self.store.lrem(key, count, value))
            elif op_name == 'rpush':
                _, key, values = op
                results.append(self.store.rpush(key, *values))
            elif op_name == 'incr':
                results.append(self.store.incr(op[1]))
            elif op_name == 'decr':
                results.append(self.store.decr(op[1]))
        
        self.operations = []
        return results
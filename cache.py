import pickle
import hashlib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar, Union

T = TypeVar('T')


class CacheStore(ABC):
    """Abstract base class for cache storage backends."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        """Get a value from the store."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        """Set a value in the store with optional TTL."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a key from the store."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists in the store."""
        pass
    
    @abstractmethod
    def lrange(self, key: str, start: int, end: int) -> List[str]:
        """Get a range of values from a list."""
        pass
    
    @abstractmethod
    def lindex(self, key: str, index: int) -> Optional[str]:
        """Get a value from a list by index."""
        pass
    
    @abstractmethod
    def lrem(self, key: str, count: int, value: str) -> int:
        """Remove elements from a list."""
        pass
    
    @abstractmethod
    def rpush(self, key: str, *values: str) -> int:
        """Append values to a list."""
        pass
    
    @abstractmethod
    def incr(self, key: str) -> int:
        """Increment a counter."""
        pass
    
    @abstractmethod
    def decr(self, key: str) -> int:
        """Decrement a counter."""
        pass
    
    @abstractmethod
    def pipeline(self) -> 'CachePipeline':
        """Create a pipeline for atomic operations."""
        pass


class CachePipeline(ABC):
    """Abstract base class for cache pipelines to perform atomic operations."""
    
    @abstractmethod
    def get(self, key: str):
        """Add a get operation to the pipeline."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: bytes, ttl: Optional[int] = None):
        """Add a set operation to the pipeline."""
        pass
    
    @abstractmethod
    def setex(self, key: str, ttl: int, value: bytes):
        """Add a setex operation to the pipeline."""
        pass
    
    @abstractmethod
    def delete(self, key: str):
        """Add a delete operation to the pipeline."""
        pass
    
    @abstractmethod
    def lrem(self, key: str, count: int, value: str):
        """Add a list remove operation to the pipeline."""
        pass
    
    @abstractmethod
    def rpush(self, key: str, *values: str):
        """Add a right push operation to the pipeline."""
        pass
    
    @abstractmethod
    def incr(self, key: str):
        """Add an increment operation to the pipeline."""
        pass
    
    @abstractmethod
    def decr(self, key: str):
        """Add a decrement operation to the pipeline."""
        pass
    
    @abstractmethod
    def execute(self) -> List[Any]:
        """Execute all operations in the pipeline."""
        pass


class Cache:
    """
    An LRU cache implementation using an abstract storage backend.
    This allows multiple Python processes across different machines to share cache data.
    """
    
    def __init__(
        self, 
        store: CacheStore,
        max_size: int = 10000,
        ttl: int = 3600  # Default TTL of 1 hour
    ):
        """
        Initialize the distributed LRU cache.
        
        Args:
            store: The storage backend to use
            max_size: Maximum number of items to store in the cache
            ttl: Default time-to-live for cache entries in seconds
        """
        # Store backend
        self.store = store
        
        # Cache configuration
        self.max_size = max_size
        self.ttl = ttl
        
        # Cache keys
        self.cache_data_prefix = "lru_cache:data:"
        self.cache_list_key = "lru_cache:keys"
        self.cache_size_key = "lru_cache:size"
        
        # Initialize cache size if it doesn't exist
        if not self.store.exists(self.cache_size_key):
            self.store.set(self.cache_size_key, b"0")
    
    def _generate_key(self, *args, **kwargs) -> str:
        """Generate a unique key based on function arguments."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])
        key_str = ":".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        data_key = f"{self.cache_data_prefix}{key}"
        
        # Check if key exists
        if not self.store.exists(data_key):
            return None
            
        # Move the key to the end of the list (most recently used)
        pipe = self.store.pipeline()
        pipe.lrem(self.cache_list_key, 1, key)
        pipe.rpush(self.cache_list_key, key)
        pipe.execute()
        
        # Get the data
        data_bytes = self.store.get(data_key)
        if data_bytes:
            return pickle.loads(data_bytes)
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        data_key = f"{self.cache_data_prefix}{key}"
        
        # Serialize the value
        value_bytes = pickle.dumps(value)
        
        # Use a pipeline to ensure atomic operations
        pipe = self.store.pipeline()
        
        # Check if the key already exists
        if not self.store.exists(data_key):
            # Get the current cache size
            size_bytes = self.store.get(self.cache_size_key)
            current_size = int(size_bytes.decode('utf-8') if size_bytes else 0)
            
            # If we've reached max size, remove the least recently used item
            if current_size >= self.max_size:
                # Get the oldest key
                oldest_key = self.store.lindex(self.cache_list_key, 0)
                if oldest_key:
                    # Remove the oldest item
                    pipe.lrem(self.cache_list_key, 1, oldest_key)
                    pipe.delete(f"{self.cache_data_prefix}{oldest_key}")
                else:
                    # If for some reason the list is empty but size > 0, reset size
                    pipe.set(self.cache_size_key, b"0")
                    current_size = 0
            
            # Increment the cache size for new items
            pipe.incr(self.cache_size_key)
        else:
            # If the key exists, remove it from the list first
            pipe.lrem(self.cache_list_key, 1, key)
        
        # Add the new key to the end of the list (most recently used)
        pipe.rpush(self.cache_list_key, key)
        
        # Set the data with TTL
        if ttl is None:
            ttl = self.ttl
        
        pipe.setex(data_key, ttl, value_bytes)
        pipe.execute()
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was deleted, False otherwise
        """
        data_key = f"{self.cache_data_prefix}{key}"
        
        # Check if key exists
        if not self.store.exists(data_key):
            return False
        
        # Use a pipeline for atomic operations
        pipe = self.store.pipeline()
        pipe.delete(data_key)
        pipe.lrem(self.cache_list_key, 1, key)
        pipe.decr(self.cache_size_key)
        pipe.execute()
        
        return True
    
    def clear(self) -> None:
        """Clear the entire cache."""
        # Get all keys
        keys = self.store.lrange(self.cache_list_key, 0, -1)
        
        # Delete all cache data
        pipe = self.store.pipeline()
        for key in keys:
            pipe.delete(f"{self.cache_data_prefix}{key}")
        
        # Reset the list and size
        pipe.delete(self.cache_list_key)
        pipe.set(self.cache_size_key, b"0")
        pipe.execute()
    
    def get_stats(self) -> Dict[str, Union[int, List[str]]]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        size_bytes = self.store.get(self.cache_size_key)
        size = int(size_bytes.decode('utf-8') if size_bytes else 0)
        keys = self.store.lrange(self.cache_list_key, 0, -1)
        
        return {
            "size": size,
            "max_size": self.max_size,
            "keys": keys
        }


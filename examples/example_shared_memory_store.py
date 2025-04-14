import time
import multiprocessing
# from memory_store import CleanMemoryStore as SharedMemoryStore  # Import your shared memory store class
# from shared_memory_store import SharedMemoryStore
from redis_store import RedisStore
from cache import DistributedLRUCache  # Import your LRU cache class

def worker_process(process_id, shared_store_params):
    """Worker function that will run in a separate process."""
    # Create a cache using the shared memory store
    store = RedisStore()
    cache = DistributedLRUCache(store=store, max_size=100, ttl=10)
    
    # Each process will set and get values
    for i in range(5):
        key = f"key_{process_id}_{i}"
        value = f"value_from_process_{process_id}_{i}"
        
        # Set a value
        print(f"Process {process_id}: Setting {key} = {value}")
        cache.set(key, value)
        
        # Sleep a bit to let other processes run
        time.sleep(0.1)
        
        # Try to read a value from another process
        other_process = (process_id + 1) % 4  # Circularly access other processes' data
        other_key = f"key_{other_process}_{i}"
        other_value = cache.get(other_key)
        print(f"Process {process_id}: Got {other_key} = {other_value}")
    
    # Test TTL expiration
    test_key = f"expiry_test_{process_id}"
    cache.set(test_key, "This will expire", ttl=2)
    print(f"Process {process_id}: Set {test_key} with 2 second TTL")
    
    # Test LRU policy by filling the cache
    print(f"Process {process_id}: Filling cache to test LRU policy")
    for i in range(150):  # More than max_size (100)
        cache.set(f"lru_test_{process_id}_{i}", f"value_{i}")
    
    # Sleep to let TTL expire
    time.sleep(3)
    
    # Check if expired key is gone
    value = cache.get(test_key)
    print(f"Process {process_id}: After TTL, {test_key} = {value}")
    
    # Get cache stats
    stats = cache.get_stats()
    print(f"Process {process_id}: Cache stats: {stats}")


if __name__ == "__main__":
    # Parameters for the shared memory store
    store_params = {
        "cleanup_interval": 5  # Clean up every 5 seconds
    }
    
    # Create and start multiple processes
    processes = []
    for i in range(5):
        p = multiprocessing.Process(target=worker_process, args=(i, store_params))
        processes.append(p)
        p.start()
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
    
    print("All processes completed")
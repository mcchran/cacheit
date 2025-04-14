from cache import DistributedLRUCache, smart_list_cache
from memory_store import CleanMemoryStore
import time

# the way to use the library
if __name__ == "__main__":
   
    # Example with in-memory storage (for testing or single process use)
    memory_store = CleanMemoryStore(cleanup_interval=5)
    memory_cache = DistributedLRUCache(
        store=memory_store,
        max_size=1000,
        ttl=60 * 60  # 1 hour
    )

    # let's createa a scenario to test the smart list cache
    @smart_list_cache(
        cache_instance=memory_cache,
        ttl=300,
        list_arg_position=1
    )
    def fetch_data_from_provider(provider_id, ids):
        """Example function that would fetch data from an external provider."""
        print(f"Fetching data from provider {provider_id} with query {ids}")
        time.sleep(1)  # Simulate API call
        result = []
        for id in ids:
            result.append(
                {
                    "id": id,
                    "result": f"{id}",
                    "timestamp": time.time()
                }
            )
        return result
    
    data = fetch_data_from_provider(1, [1, 2, 3])
    print("First call result:", data)
    # Second call should hit the cache
    data = fetch_data_from_provider(1, [2, 4])
    print("Second call result:", data)
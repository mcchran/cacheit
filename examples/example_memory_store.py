from cache import Cache
from decos import lru_cache
# from memory_store import CleanMemoryStore
from memory_store import CleanMemoryStore
import time

# the way to use the library
if __name__ == "__main__":
   
    # Example with in-memory storage (for testing or single process use)
    memory_store = CleanMemoryStore(cleanup_interval=5)
    memory_cache = Cache(
        store=memory_store,
        max_size=1000,
        ttl=60 * 60  # 1 hour
    )
   

    # decorator useage with emeory store
    @lru_cache(ttl=300, cache_instance=memory_cache)
    def fetch_data_from_provider(provider_id, query):
        """Example function that would fetch data from an external provider."""
        print(f"Fetching data from provider {provider_id} with query {query}")
        time.sleep(1)  # Simulate API call
        return {
            "provider": provider_id,
            "query": query,
            "result": f"Data for {query} from provider {provider_id}",
            "timestamp": time.time()
        }
    
    # First call will miss the cache
    result1 = fetch_data_from_provider(1, "test")
    print("First call result:", result1)
    
    # Second call should hit the cache
    result2 = fetch_data_from_provider(1, "test")
    print("Second call result:", result2)

    # decoearot useage with memory store and tt1 1 sec
    @lru_cache(ttl=1, cache_instance=memory_cache)
    def fetch_data_from_provider(provider_id, query):
        """Example function that would fetch data from an external provider."""
        print(f"Fetching data from provider {provider_id} with query {query}")
        time.sleep(1)  # Simulate API call
        return {
            "provider": provider_id,
            "query": query,
            "result": f"Data for {query} from provider {provider_id}",
            "timestamp": time.time()
        }
    
    result1 = fetch_data_from_provider(2, "tester")
    print("First call result:", result1)

    # let's sleep to make sure we have a cache miss after 1 sec
    time.sleep(1)
    result2 = fetch_data_from_provider(2, "tester")
    print("Second call result:", result2)
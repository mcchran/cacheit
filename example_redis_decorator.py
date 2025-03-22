import time
import multiprocessing
from redis_store import RedisStore
from cache import DistributedLRUCache, lru_cache
# let's define a distributed lru cache with redis store
memory_store = RedisStore()
memory_cache = DistributedLRUCache(
    store=memory_store,
    max_size=1000,
    ttl=60 * 60  # 1 hour
)

# let's create decorate a function with the lru_cache decorator
@lru_cache(ttl=300, cache_instance=memory_cache)
def fetch_data(provider_id, query):
    """Example function that would fetch data from an external provider."""
    print(f"Fetching data from provider {provider_id} with query {query}")
    time.sleep(1)
    return {
        "provider": provider_id,
        "query": query,
        "result": f"Data for {query} from provider {provider_id}",
        "timestamp": time.time()
    }

# let's create a producer and consumer process
def producer_process():
    for i in range(5):
        result = fetch_data(1, f"query_{i}")
        print(f"Producer: Got result for query_{i}: {result}")
        time.sleep(0.5)


def consumer_process():
    for i in range(5):
        result = fetch_data(1, f"query_{i}")
        print(f"Consumer: Got result for query_{i}: {result}")
        time.sleep(1)

if __name__ == "__main__":
    producer = multiprocessing.Process(target=producer_process)
    consumer = multiprocessing.Process(target=consumer_process)

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()
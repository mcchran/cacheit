# let's list all decorators to help us use the module at ease
from functools import wraps
from typing import Callable, Any, Optional

from cache import Cache

def lru_cache(
    function: Optional[Callable] = None,
    ttl: Optional[int] = None,
    key_prefix: str = "",
    cache_instance: Optional[Cache] = None
) -> Callable:
    """
    Decorator to use the distributed LRU cache.
    
    Args:
        function: Function to decorate
        ttl: Time-to-live for cached values
        key_prefix: Prefix for cache keys
        cache_instance: Existing DistributedLRUCache instance to use
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        # Use provided cache instance or create a default one with Redis
        nonlocal cache_instance
        if cache_instance is None:
            # should avoud reaching to that point
            from memory_store import MemoryStore
            store = MemoryStore()
            cache_instance = DistributedLRUCache(store=store)
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Generate a cache key
            cache_key = f"{key_prefix}:{func.__name__}:{cache_instance._generate_key(*args, **kwargs)}"
            
            # Try to get from cache
            cached_result = cache_instance.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # If not in cache, call the function
            print(f"Cache miss for key: {cache_key}")
            result = func(*args, **kwargs)
            
            # Cache the result
            cache_instance.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    
    # Handle both @lru_cache and @lru_cache(...)
    if function is not None:
        return decorator(function)
    return decorator


def smart_list_cache(
    function: Optional[Callable] = None,
    ttl: Optional[int] = None,
    key_prefix: str = "",
    cache_instance: Optional["DistributedLRUCache"] = None,
    list_arg_position: int = 0,  # Position of the ids list argument
    id_extractor: Optional[Callable] = (lambda x: x.id if hasattr(x, 'id') else x.get('id'))  # Function to extract ids from results
) -> Callable:
    """
    Enhanced LRU cache decorator that optimizes provider queries by only fetching uncached ids.
    
    Args:
        function: Function to decorate
        ttl: Time-to-live for cached values
        key_prefix: Prefix for cache keys
        cache_instance: Existing DistributedLRUCache instance to use
        list_arg_position: Position of the argument containing the list of ids
        id_extractor: Function to extract id from a result object (if None, assumes id is a direct field)
        
    Returns:
        Decorated function that optimizes provider calls
    """

    def decorator(func: Callable) -> Callable:
        # Initialize cache instance
        nonlocal cache_instance
        # TODO: that should not be optional ... let's move that under the required ones!
        if cache_instance is None:
            from memory_store import MemoryStore
            store = MemoryStore()
            cache_instance = DistributedLRUCache(store=store)
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get the list of ids from arguments
            if list_arg_position < len(args) and isinstance(args[list_arg_position], list):
                id_list = args[list_arg_position]
                
                # If empty list, return empty result immediately
                if not id_list:
                    return []
                
                # Check cache for each id
                cached_results = {}
                uncached_ids = []
                
                for item_id in id_list:
                    # Generate cache key for this specific id
                    args_copy = list(args)
                    args_copy[list_arg_position] = [item_id]  # Wrap in list to maintain expected structure
                    item_cache_key = f"{key_prefix}:{func.__name__}:{item_id}"
                    
                    # Try to get from cache
                    cached_result = cache_instance.get(item_cache_key)
                    if cached_result is not None:
                        cached_results[item_id] = cached_result
                    else:
                        uncached_ids.append(item_id)
                
                # If all ids were cached, combine and return
                if not uncached_ids:
                    print(f"Complete cache hit for all {len(id_list)} ids")
                    return [cached_results[item_id] for item_id in id_list]
                
                # Make a single DB query for only the uncached ids
                print(f"Partial cache hit. Fetching {len(uncached_ids)}/{len(id_list)} ids from provider.")
                
                # Replace original id list with only uncached ids
                modified_args = list(args)
                modified_args[list_arg_position] = uncached_ids
                
                # Execute function with only the uncached ids
                provider_results = func(*modified_args, **kwargs)
                
                # FIXME: there is pitfall here! if we have just a single response of a dict ... 
                # we will treat that as an iterable and will fail... 
                # Cache individual results
                for result in provider_results:
                    result_id = id_extractor(result)
                    
                    # Cache this individual result
                    item_cache_key = f"{key_prefix}:{func.__name__}:{result_id}"
                    cache_instance.set(item_cache_key, result, ttl)
                    
                    # Add to our results dictionary
                    cached_results[result_id] = result
                
                # Combine and return results in original id order
                combined_results = []
                for item_id in id_list:
                    if item_id in cached_results:
                        combined_results.append(cached_results[item_id])
                
                return combined_results
            
            # Non-list argument case - use standard caching
            cache_key = f"{key_prefix}:{func.__name__}:{cache_instance._generate_key(*args, **kwargs)}"
            cached_result = cache_instance.get(cache_key)
            
            if cached_result is not None:
                return cached_result
            
            print(f"Cache miss for key: {cache_key}")
            result = func(*args, **kwargs)
            cache_instance.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    
    # Handle both @smart_list_cache and @smart_list_cache(...)
    if function is not None:
        return decorator(function)
    return decorator


# let's crate the smart_dict_cache decorator that does the same as the smart_list_cache but return dicts
def smart_dict_cache(
    function: Optional[Callable] = None,
    ttl: Optional[int] = None,
    key_prefix: str = "",
    cache_instance: Optional["DistributedLRUCache"] = None,
    list_arg_position: int = 0,  # Position of the ids list argument
    id_extractor: Optional[Callable] = (lambda x: x)  # Function to extract ids from results
) -> Callable:
    """
    Enhanced LRU cache decorator that optimizes provider queries by only fetching uncached ids.
    
    Args:
        function: Function to decorate
        ttl: Time-to-live for cached values
        key_prefix: Prefix for cache keys
        cache_instance: Existing DistributedLRUCache instance to use
        id_extractor: Function to extract id from a result object (if None, assumes id is a direct field)
        
    Returns:
        Decorated function that optimizes provider calls
    """
 
    def decorator(func: Callable) -> Callable:
        # Initialize cache instance
        nonlocal cache_instance
        # TODO: that should not be optional ... let's move that under the required ones!
        if cache_instance is None:
            from memory_store import MemoryStore
            store = MemoryStore()
            cache_instance = DistributedLRUCache(store=store)
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get the list of ids from arguments
            if list_arg_position < len(args) and isinstance(args[list_arg_position], list):
                id_list = args[list_arg_position]
                
                # If empty list, return empty result immediately
                if not id_list:
                    return []
                
                # Check cache for each id
                cached_results = {}
                uncached_ids = []
                
                for item_id in id_list:
                    # Generate cache key for this specific id
                    args_copy = list(args)
                    args_copy[list_arg_position] = [item_id]  # Wrap in list to maintain expected structure
                    item_cache_key = f"{key_prefix}:{func.__name__}:{item_id}"
                    
                    # Try to get from cache
                    cached_result = cache_instance.get(item_cache_key)
                    if cached_result is not None:
                        cached_results[item_id] = cached_result
                    else:
                        uncached_ids.append(item_id)
                
                # If all ids were cached, combine and return
                if not uncached_ids:
                    print(f"Complete cache hit for all {len(id_list)} ids")
                    return {item_id: cached_results[item_id] for item_id in id_list}
                
                # Make a single DB query for only the uncached ids
                print(f"Partial cache hit. Fetching {len(uncached_ids)}/{len(id_list)} ids from provider.")
                
                # Replace original id list with only uncached ids
                modified_args = list(args)
                modified_args[list_arg_position] = uncached_ids
                
                # Execute function with only the uncached ids
                provider_results = func(*modified_args, **kwargs)
                
                # FIXME: there is pitfall here! if we have just a single response of a dict ... 
                # we will treat that as an iterable and will fail... 
                # Cache individual results
                for result in provider_results:
                    result_id = id_extractor(result)
                    
                    # Cache this individual result
                    item_cache_key = f"{key_prefix}:{func.__name__}:{result_id}"
                    cache_instance.set(item_cache_key, result, ttl)
                    
                    # Add to our results dictionary
                    cached_results[result_id] = result
                
                # Combine and return results in original id order
                combined_results = {}
                for item_id in id_list:
                    if item_id in cached_results:
                        combined_results[item_id] = cached_results[item_id]
                
                return combined_results
            
            # Non-list argument case - use standard caching
            cache_key = f"{key_prefix}:{func.__name__}:{cache_instance._generate_key(*args, **kwargs)}"
            cached_result = cache_instance.get(cache_key)
            
            if cached_result is not None:
                return cached_result
            
            print(f"Cache miss for key: {cache_key}")
            result = func(*args, **kwargs)
            cache_instance.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    
    # Handle both @smart_list_cache and @smart_list_cache(...)
    if function is not None:
        return decorator(function)
    return decorator


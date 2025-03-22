
from cache import CachePipeline, CacheStore
from typing import Any, List, Optional

class RedisStore(CacheStore):
    """Redis implementation of the CacheStore interface."""
    
    def __init__(
        self, 
        host: str = 'localhost', 
        port: int = 6379, 
        db: int = 0,
        password: Optional[str] = None
    ):
        """Initialize Redis connection."""
        import redis
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=False  # We need binary responses for pickle data
        )
    
    def get(self, key: str) -> Optional[bytes]:
        return self.redis.get(key)
    
    def set(self, key: str, value: bytes, ttl: Optional[int] = None) -> bool:
        if ttl is not None:
            return bool(self.redis.setex(key, ttl, value))
        return bool(self.redis.set(key, value))
    
    def delete(self, key: str) -> bool:
        return bool(self.redis.delete(key))
    
    def exists(self, key: str) -> bool:
        return bool(self.redis.exists(key))
    
    def lrange(self, key: str, start: int, end: int) -> List[str]:
        result = self.redis.lrange(key, start, end)
        return [x.decode('utf-8') if isinstance(x, bytes) else x for x in result]
    
    def lindex(self, key: str, index: int) -> Optional[str]:
        result = self.redis.lindex(key, index)
        if result is not None and isinstance(result, bytes):
            return result.decode('utf-8')
        return result
    
    def lrem(self, key: str, count: int, value: str) -> int:
        return self.redis.lrem(key, count, value)
    
    def rpush(self, key: str, *values: str) -> int:
        return self.redis.rpush(key, *values)
    
    def incr(self, key: str) -> int:
        return self.redis.incr(key)
    
    def decr(self, key: str) -> int:
        return self.redis.decr(key)
    
    def pipeline(self) -> 'CachePipeline':
        return RedisPipeline(self.redis.pipeline())


class RedisPipeline(CachePipeline):
    """Redis implementation of the CachePipeline interface."""
    
    def __init__(self, pipeline):
        self.pipeline = pipeline
    
    def get(self, key: str):
        self.pipeline.get(key)
        return self
    
    def set(self, key: str, value: bytes, ttl: Optional[int] = None):
        if ttl is not None:
            self.pipeline.setex(key, ttl, value)
        else:
            self.pipeline.set(key, value)
        return self
    
    def setex(self, key: str, ttl: int, value: bytes):
        self.pipeline.setex(key, ttl, value)
        return self
    
    def delete(self, key: str):
        self.pipeline.delete(key)
        return self
    
    def lrem(self, key: str, count: int, value: str):
        self.pipeline.lrem(key, count, value)
        return self
    
    def rpush(self, key: str, *values: str):
        self.pipeline.rpush(key, *values)
        return self
    
    def incr(self, key: str):
        self.pipeline.incr(key)
        return self
    
    def decr(self, key: str):
        self.pipeline.decr(key)
        return self
    
    def execute(self) -> List[Any]:
        return self.pipeline.execute()
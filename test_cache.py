import unittest
from unittest.mock import patch, MagicMock
import pickle
import hashlib
from cache import Cache

class TestCache(unittest.TestCase):
    @patch('cache.CacheStore')
    def setUp(self, mock_cache_store):
        self.mock_store = MagicMock()
        self.mock_store.exists.return_value = False
        self.cache = Cache(
            store=self.mock_store,
            max_size=100,
            ttl=60,  # 1 minute
        )
        
    def test_init(self):
        """Test the initialization of the cache."""
        # Test that cache initializes correctly
        self.assertEqual(self.cache.max_size, 100)
        self.assertEqual(self.cache.ttl, 60)
        self.assertEqual(self.cache.cache_data_prefix, "lru_cache:data:")
        self.assertEqual(self.cache.cache_list_key, "lru_cache:keys")
        self.assertEqual(self.cache.cache_size_key, "lru_cache:size")
        
        # Verify that it initializes the cache size if it doesn't exist
        self.mock_store.exists.assert_called_with(self.cache.cache_size_key)
        self.mock_store.set.assert_called_with(self.cache.cache_size_key, b"0")
        
    def test_get_not_found(self):
        """Test getting a non-existent key from the cache."""
        self.mock_store.exists.return_value = False
        result = self.cache.get("non_existent_key")
        self.assertIsNone(result)
        self.mock_store.exists.assert_called_with("lru_cache:data:non_existent_key")
        
    def test_get_found(self):
        """Test getting an existing key from the cache."""
        # Mock the key exists
        self.mock_store.exists.return_value = True
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Mock the data return value
        test_data = {"name": "test_value"}
        self.mock_store.get.return_value = pickle.dumps(test_data)
        
        # Call get
        result = self.cache.get("test_key")
        
        # Assert result is correct
        self.assertEqual(result, test_data)
        
        # Verify operations
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        self.mock_store.get.assert_called_with("lru_cache:data:test_key")
        
        # Verify pipeline operations
        mock_pipeline.lrem.assert_called_with(self.cache.cache_list_key, 1, "test_key")
        mock_pipeline.rpush.assert_called_with(self.cache.cache_list_key, "test_key")
        mock_pipeline.execute.assert_called_once()
        
    def test_set_new_key(self):
        """Test setting a new key in the cache."""
        # Mock key doesn't exist
        self.mock_store.exists.return_value = False
        
        # Mock current size
        self.mock_store.get.return_value = b"50"
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Test data
        test_data = {"name": "test_value"}
        
        # Call set
        self.cache.set("test_key", test_data)
        
        # Verify operations
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        self.mock_store.get.assert_called_with(self.cache.cache_size_key)
        
        # Verify pipeline operations
        mock_pipeline.incr.assert_called_with(self.cache.cache_size_key)
        mock_pipeline.rpush.assert_called_with(self.cache.cache_list_key, "test_key")
        mock_pipeline.setex.assert_called_with(
            "lru_cache:data:test_key", 
            60, 
            pickle.dumps(test_data)
        )
        mock_pipeline.execute.assert_called_once()
        
    def test_set_existing_key(self):
        """Test updating an existing key in the cache."""
        # Mock key exists
        self.mock_store.exists.return_value = True
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Test data
        test_data = {"name": "updated_value"}
        
        # Call set
        self.cache.set("test_key", test_data)
        
        # Verify operations
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        
        # Verify pipeline operations
        mock_pipeline.lrem.assert_called_with(self.cache.cache_list_key, 1, "test_key")
        mock_pipeline.rpush.assert_called_with(self.cache.cache_list_key, "test_key")
        mock_pipeline.setex.assert_called_with(
            "lru_cache:data:test_key", 
            60, 
            pickle.dumps(test_data)
        )
        mock_pipeline.execute.assert_called_once()
        
    def test_set_custom_ttl(self):
        """Test setting a key with custom TTL."""
        # Mock key doesn't exist
        self.mock_store.exists.return_value = False
        
        # Mock current size
        self.mock_store.get.return_value = b"50"
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Test data
        test_data = {"name": "test_value"}
        custom_ttl = 120  # 2 minutes
        
        # Call set with custom TTL
        self.cache.set("test_key", test_data, ttl=custom_ttl)
        
        # Verify operations
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        
        # Verify pipeline operations with custom TTL
        mock_pipeline.setex.assert_called_with(
            "lru_cache:data:test_key", 
            custom_ttl, 
            pickle.dumps(test_data)
        )
        mock_pipeline.execute.assert_called_once()
        
    def test_set_eviction(self):
        """Test LRU eviction when the cache is full."""
        # Mock key doesn't exist
        self.mock_store.exists.return_value = False
        
        # Mock current size at max
        self.mock_store.get.return_value = b"100"
        
        # Mock oldest key
        self.mock_store.lindex.return_value = "oldest_key"
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Test data
        test_data = {"name": "test_value"}
        
        # Call set
        self.cache.set("test_key", test_data)
        
        # Verify operations
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        self.mock_store.get.assert_called_with(self.cache.cache_size_key)
        self.mock_store.lindex.assert_called_with(self.cache.cache_list_key, 0)
        
        # Verify eviction operations
        mock_pipeline.lrem.assert_any_call(self.cache.cache_list_key, 1, "oldest_key")
        mock_pipeline.delete.assert_called_with("lru_cache:data:oldest_key")
        mock_pipeline.incr.assert_called_with(self.cache.cache_size_key)
        mock_pipeline.rpush.assert_called_with(self.cache.cache_list_key, "test_key")
        mock_pipeline.execute.assert_called_once()
        
    def test_delete_not_found(self):
        """Test deleting a non-existent key."""
        # Mock key doesn't exist
        self.mock_store.exists.return_value = False
        
        # Call delete
        result = self.cache.delete("non_existent_key")
        
        # Verify result and operations
        self.assertFalse(result)
        self.mock_store.exists.assert_called_with("lru_cache:data:non_existent_key")
        
    def test_delete_found(self):
        """Test deleting an existing key."""
        # Mock key exists
        self.mock_store.exists.return_value = True
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Call delete
        result = self.cache.delete("test_key")
        
        # Verify result and operations
        self.assertTrue(result)
        self.mock_store.exists.assert_called_with("lru_cache:data:test_key")
        
        # Verify pipeline operations
        mock_pipeline.delete.assert_called_with("lru_cache:data:test_key")
        mock_pipeline.lrem.assert_called_with(self.cache.cache_list_key, 1, "test_key")
        mock_pipeline.decr.assert_called_with(self.cache.cache_size_key)
        mock_pipeline.execute.assert_called_once()
        
    def test_clear(self):
        """Test clearing the entire cache."""
        # Mock list of keys
        mock_keys = ["key1", "key2", "key3"]
        self.mock_store.lrange.return_value = mock_keys
        
        # Create a pipeline mock
        mock_pipeline = MagicMock()
        self.mock_store.pipeline.return_value = mock_pipeline
        
        # Call clear
        self.cache.clear()
        
        # Verify operations
        self.mock_store.lrange.assert_called_with(self.cache.cache_list_key, 0, -1)
        
        # Verify pipeline operations
        mock_pipeline.delete.assert_any_call("lru_cache:data:key1")
        mock_pipeline.delete.assert_any_call("lru_cache:data:key2")
        mock_pipeline.delete.assert_any_call("lru_cache:data:key3")
        mock_pipeline.delete.assert_any_call(self.cache.cache_list_key)
        mock_pipeline.set.assert_called_with(self.cache.cache_size_key, b"0")
        mock_pipeline.execute.assert_called_once()
        
    def test_get_stats(self):
        """Test getting cache statistics."""
        # Mock size
        self.mock_store.get.return_value = b"75"
        
        # Mock keys
        mock_keys = [b"key1", b"key2", b"key3"]
        self.mock_store.lrange.return_value = mock_keys
        
        # Call get_stats
        stats = self.cache.get_stats()
        
        # Verify operations
        self.mock_store.get.assert_called_with(self.cache.cache_size_key)
        self.mock_store.lrange.assert_called_with(self.cache.cache_list_key, 0, -1)
        
        # Verify results
        expected_stats = {
            "size": 75,
            "max_size": 100,
            "keys": mock_keys
        }
        self.assertEqual(stats, expected_stats)
        
    def test_generate_key(self):
        """Test key generation from arguments."""
        # Test simple arguments
        key1 = self.cache._generate_key("arg1", "arg2")
        expected_key1 = hashlib.md5("arg1:arg2".encode()).hexdigest()
        self.assertEqual(key1, expected_key1)
        
        # Test with kwargs
        key2 = self.cache._generate_key("arg1", param1="value1", param2="value2")
        expected_key2 = hashlib.md5("arg1:param1:value1:param2:value2".encode()).hexdigest()
        self.assertEqual(key2, expected_key2)
    
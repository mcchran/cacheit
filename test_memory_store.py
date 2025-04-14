from memory_store import MemoryStore, MemoryPipeline
from unittest.mock import patch

import unittest
import time

class TestMemoryStore(unittest.TestCase):
    def setUp(self):
        """Set up a new MemoryStore instance before each test."""
        self.store = MemoryStore()

    def test_init(self):
        """Test that initialization creates empty data structures."""
        self.assertEqual(self.store.data, {})
        self.assertEqual(self.store.lists, {})
        self.assertEqual(self.store.expiry, {})
        self.assertEqual(self.store.counters, {})

    def test_set_get(self):
        """Test setting and getting values."""
        # Set a value
        key = "test_key"
        value = b"test_value"
        self.assertTrue(self.store.set(key, value))

        # Verify it can be retrieved
        self.assertEqual(self.store.get(key), value)

        # Test with a different value
        new_value = b"new_value"
        self.assertTrue(self.store.set(key, new_value))
        self.assertEqual(self.store.get(key), new_value)

        # Test non-existent key
        self.assertIsNone(self.store.get("nonexistent_key"))

    def test_set_with_ttl(self):
        """Test setting values with TTL."""
        key = "ttl_key"
        value = b"ttl_value"

        # Set with TTL
        self.store.set(key, value, ttl=2)

        # Check value exists immediately
        self.assertEqual(self.store.get(key), value)

        # Check expiry time was set
        self.assertIn(key, self.store.expiry)
        self.assertGreater(self.store.expiry[key], time.time())

        # Check TTL removal when setting without TTL
        self.store.set(key, value)
        self.assertNotIn(key, self.store.expiry)

    @patch("time.time")
    def test_expiry(self, mock_time):
        """Test that keys expire correctly."""
        mock_time.return_value = 100  # Fixed time for testing

        # Set a key with 10 second TTL
        self.store.set("expires_soon", b"value", ttl=10)
        self.assertEqual(self.store.expiry["expires_soon"], 110)

        # Still valid
        mock_time.return_value = 105
        self.assertFalse(self.store._check_expiry("expires_soon"))
        self.assertEqual(self.store.get("expires_soon"), b"value")

        # Expired
        mock_time.return_value = 111
        self.assertTrue(self.store._check_expiry("expires_soon"))
        self.assertIsNone(self.store.get("expires_soon"))
        self.assertNotIn("expires_soon", self.store.data)
        self.assertNotIn("expires_soon", self.store.expiry)

    def test_delete(self):
        """Test deleting keys."""
        # Set up test data
        self.store.set("key1", b"value1")
        self.store.set("key2", b"value2", ttl=100)

        # Delete existing key
        self.assertTrue(self.store.delete("key1"))
        self.assertIsNone(self.store.get("key1"))

        # Delete key with expiry
        self.assertTrue(self.store.delete("key2"))
        self.assertIsNone(self.store.get("key2"))
        self.assertNotIn("key2", self.store.expiry)

        # Delete non-existent key
        self.assertFalse(self.store.delete("nonexistent"))

    def test_exists(self):
        """Test checking if keys exist."""
        # Set up test data
        self.store.set("key1", b"value1")
        self.store.set("key2", b"value2", ttl=100)
        # Check existing keys
        self.assertTrue(self.store.exists("key1"))
        self.assertTrue(self.store.exists("key2"))

        # Check non-existent key
        self.assertFalse(self.store.exists("nonexistent"))

        # Check expired key
        with patch("memory_store.time") as mock_time:
            mock_time.time.return_value = time.time() + 200
            self.assertFalse(self.store.exists("key2"))

    def test_rpush_lrange(self):
        """Test adding to list and retrieving ranges."""
        # Simple case
        self.assertEqual(self.store.rpush("list1", "value1"), 1)
        self.assertEqual(self.store.lrange("list1", 0, -1), ["value1"])

        # Multiple values
        self.assertEqual(self.store.rpush("list1", "value2", "value3"), 3)
        self.assertEqual(
            self.store.lrange("list1", 0, -1), ["value1", "value2", "value3"]
        )

        # Subsection of list
        self.assertEqual(self.store.lrange("list1", 1, 2), ["value2", "value3"])

        # Empty list
        self.assertEqual(self.store.lrange("nonexistent", 0, -1), [])

    def test_lindex(self):
        """Test retrieving item at specific index."""
        # Set up test data
        self.store.rpush("list1", "value1", "value2", "value3")

        # Test valid indices
        self.assertEqual(self.store.lindex("list1", 0), "value1")
        self.assertEqual(self.store.lindex("list1", 1), "value2")
        self.assertEqual(self.store.lindex("list1", 2), "value3")

        # Test negative index
        self.assertEqual(self.store.lindex("list1", -1), "value3")

        # Test out of range index
        self.assertIsNone(self.store.lindex("list1", 10))

        # Test non-existent list
        self.assertIsNone(self.store.lindex("nonexistent", 0))

    def test_lrem_positive_count(self):
        """Test removing elements from list (positive count)."""
        # Prepare list with duplicates
        self.store.rpush("list1", "a", "b", "a", "c", "a", "d")

        # Remove first 2 occurrences of 'a'
        self.assertEqual(self.store.lrem("list1", 2, "a"), 2)
        self.assertEqual(self.store.lrange("list1", 0, -1), ["b", "c", "a", "d"])

        # Try to remove more than exist
        self.assertEqual(self.store.lrem("list1", 2, "a"), 1)
        self.assertEqual(self.store.lrange("list1", 0, -1), ["b", "c", "d"])

        # Try to remove non-existent value
        self.assertEqual(self.store.lrem("list1", 1, "z"), 0)

        # Try non-existent list
        self.assertEqual(self.store.lrem("nonexistent", 1, "a"), 0)

    def test_lrem_negative_count(self):
        """Test removing elements from list (negative count)."""
        # Prepare list with duplicates
        self.store.rpush("list1", "a", "b", "a", "c", "a", "d")

        # Remove last 2 occurrences of 'a'
        self.assertEqual(self.store.lrem("list1", -2, "a"), 2)
        self.assertEqual(self.store.lrange("list1", 0, -1), ["a", "b", "c", "d"])

    def test_lrem_zero_count(self):
        """Test removing all occurrences (count=0)."""
        # Prepare list with duplicates
        self.store.rpush("list1", "a", "b", "a", "c", "a", "d")

        # Remove all occurrences of 'a'
        self.assertEqual(self.store.lrem("list1", 0, "a"), 3)
        self.assertEqual(self.store.lrange("list1", 0, -1), ["b", "c", "d"])

    def test_incr_decr(self):
        """Test incrementing and decrementing counters."""
        # New counter
        self.assertEqual(self.store.incr("counter1"), 1)
        self.assertEqual(self.store.incr("counter1"), 2)

        # Decrement
        self.assertEqual(self.store.decr("counter1"), 1)
        self.assertEqual(self.store.decr("counter1"), 0)
        self.assertEqual(self.store.decr("counter1"), -1)

        # New counter with decrement first
        self.assertEqual(self.store.decr("counter2"), -1)

    def test_pipeline(self):
        """Test getting a pipeline instance."""
        pipeline = self.store.pipeline()
        self.assertIsInstance(pipeline, MemoryPipeline)
        self.assertEqual(pipeline.store, self.store)

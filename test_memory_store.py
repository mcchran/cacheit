from memory_store import MemoryStore, MemoryPipeline
from unittest.mock import patch, Mock

import unittest
import threading
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


class TestMemoryPipeline(unittest.TestCase):
    """Unit tests for the thread-safe MemoryPipeline class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.store = MemoryStore()
        self.pipeline = MemoryPipeline(self.store)

    def test_init(self):
        """Test initialization of the MemoryPipeline."""
        self.assertEqual(self.pipeline.operations, [])
        self.assertIs(self.pipeline.store, self.store)
        # let's assert we have a lock attached -- that is tricky
        # caouse the threading.RLock comes from a c binding and returns an _thread.Rlock type
        self.assertIsInstance(self.pipeline._lock, type(threading.RLock()))

    def test_get_operation(self):
        """Test adding a get operation to the pipeline."""
        result = self.pipeline.get("test_key")
        self.assertEqual(self.pipeline.operations, [("get", "test_key")])
        self.assertIs(result, self.pipeline)  # Should return self for chaining

    def test_set_operation(self):
        """Test adding a set operation to the pipeline."""
        value = b"test_value"
        result = self.pipeline.set("test_key", value, 3600)
        self.assertEqual(self.pipeline.operations, [("set", "test_key", value, 3600)])
        self.assertIs(result, self.pipeline)

    def test_setex_operation(self):
        """Test adding a setex operation to the pipeline."""
        value = b"test_value"
        result = self.pipeline.setex("test_key", 3600, value)
        self.assertEqual(self.pipeline.operations, [("set", "test_key", value, 3600)])
        self.assertIs(result, self.pipeline)

    def test_delete_operation(self):
        """Test adding a delete operation to the pipeline."""
        result = self.pipeline.delete("test_key")
        self.assertEqual(self.pipeline.operations, [("delete", "test_key")])
        self.assertIs(result, self.pipeline)

    def test_lrem_operation(self):
        """Test adding an lrem operation to the pipeline."""
        result = self.pipeline.lrem("test_key", 1, "value")
        self.assertEqual(self.pipeline.operations, [("lrem", "test_key", 1, "value")])
        self.assertIs(result, self.pipeline)

    def test_rpush_operation(self):
        """Test adding an rpush operation to the pipeline."""
        result = self.pipeline.rpush("test_key", "value1", "value2")
        self.assertEqual(
            self.pipeline.operations, [("rpush", "test_key", ("value1", "value2"))]
        )
        self.assertIs(result, self.pipeline)

    def test_incr_operation(self):
        """Test adding an incr operation to the pipeline."""
        result = self.pipeline.incr("test_key")
        self.assertEqual(self.pipeline.operations, [("incr", "test_key")])
        self.assertIs(result, self.pipeline)

    def test_decr_operation(self):
        """Test adding a decr operation to the pipeline."""
        result = self.pipeline.decr("test_key")
        self.assertEqual(self.pipeline.operations, [("decr", "test_key")])
        self.assertIs(result, self.pipeline)

    def test_execute_empty_operations(self):
        """Test execute with no operations."""
        results = self.pipeline.execute()
        self.assertEqual(results, [])
        self.assertEqual(self.pipeline.operations, [])

    def test_execute_operations(self):
        """Test executing a mix of operations."""
        # Setup pipeline with various operations
        value = b"test_value"
        self.pipeline.get("key1").set("key2", value, 3600).delete("key3")

        # Mock the store methods to verify they're called correctly
        with patch.object(
            self.store, "get", return_value=value
        ) as mock_get, patch.object(
            self.store, "set", return_value=True
        ) as mock_set, patch.object(
            self.store, "delete", return_value=True
        ) as mock_delete:

            results = self.pipeline.execute()

            # Check results and method calls
            self.assertEqual(results, [value, True, True])
            mock_get.assert_called_once_with("key1")
            mock_set.assert_called_once_with("key2", value, 3600)
            mock_delete.assert_called_once_with("key3")

        # Operations should be cleared after execute
        self.assertEqual(self.pipeline.operations, [])

    def test_operations_cleared_after_execute(self):
        """Test that operations list is cleared after execute."""
        self.pipeline.get("test_key")
        self.pipeline.execute()
        self.assertEqual(self.pipeline.operations, [])

    def test_chained_operations(self):
        """Test that operations can be chained."""
        value = b"test_value"
        self.pipeline.get("key1").set("key2", value).incr("key3").decr("key4")

        expected_operations = [
            ("get", "key1"),
            ("set", "key2", value, None),
            ("incr", "key3"),
            ("decr", "key4"),
        ]

        self.assertEqual(self.pipeline.operations, expected_operations)

    def test_thread_safety(self):
        """Test thread safety by having multiple threads add operations."""

        def add_operations(pipeline, prefix, count):
            for i in range(count):
                pipeline.set(f"{prefix}_key_{i}", f"value_{i}".encode())
                pipeline.get(f"{prefix}_key_{i}")

        threads = []
        num_threads = 10
        operations_per_thread = 100

        # Start multiple threads to add operations
        for i in range(num_threads):
            thread = threading.Thread(
                target=add_operations,
                args=(self.pipeline, f"thread_{i}", operations_per_thread),
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that all operations were added
        total_expected_operations = (
            num_threads * operations_per_thread * 2
        )  # Each thread adds 2 operations per iteration
        self.assertEqual(len(self.pipeline.operations), total_expected_operations)

        # Execute and verify operations are cleared
        self.pipeline.execute()
        self.assertEqual(len(self.pipeline.operations), 0)

    def test_concurrent_execute(self):
        """Test concurrent execute calls."""
        # Add some initial operations
        for i in range(50):
            self.pipeline.set(f"key_{i}", f"value_{i}".encode())

        results = []
        execution_counts = []

        def execute_and_add_operations(pipeline, prefix, count):
            # Execute current operations
            res = pipeline.execute()
            results.append(len(res))

            # Add new operations
            for i in range(count):
                pipeline.set(f"{prefix}_key_{i}", f"value_{i}".encode())
                pipeline.get(f"{prefix}_key_{i}")

            # Execute again and record how many operations were executed
            res = pipeline.execute()
            execution_counts.append(len(res))

        threads = []
        num_threads = 5
        operations_per_thread = 20

        # Start multiple threads to execute and add operations
        for i in range(num_threads):
            thread = threading.Thread(
                target=execute_and_add_operations,
                args=(self.pipeline, f"concurrent_{i}", operations_per_thread),
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify operations are cleared after all executions
        self.assertEqual(len(self.pipeline.operations), 0)

        # Verify that all added operations were executed
        total_operations_executed = sum(results) + sum(execution_counts)
        expected_operations = 50 + (num_threads * operations_per_thread * 2)
        self.assertEqual(total_operations_executed, expected_operations)

    def test_execute_with_exception(self):
        """Test behavior when store methods raise exceptions."""
        # Setup pipeline with operations
        self.pipeline.get("key1").set("key2", b"value2")

        # Make the store's get method raise an exception
        self.store.get = Mock(side_effect=Exception("Test exception"))

        # Execute should continue processing operations despite exceptions
        with self.assertRaises(Exception):
            self.pipeline.execute()

        # Operations should still be cleared
        self.assertEqual(self.pipeline.operations, [])

    def test_lock_reentrance(self):
        """Test that the RLock is reentrant (same thread can acquire multiple times)."""
        # This is testing internal implementation, so we'll manipulate the lock directly
        self.pipeline._lock.acquire()
        try:
            # This should not block since RLock is reentrant
            self.pipeline._lock.acquire()
            self.pipeline._lock.release()
        finally:
            self.pipeline._lock.release()

        # If we get here without deadlock, the test passes

from multiprocessing import Process
from multiprocessing.managers import BaseManager

class SharedObject:
    """A simple wrapper for objects to be shared via Manager."""
    def __init__(self):
        self.data = {}       # Dictionary for general data storage
        self.lists = {}      # Dictionary of lists
        self.expiry = {}     # Dictionary for expiry tracking
        self.counters = {}   # Dictionary for counters

    # Methods to interact with `data`
    def set_data(self, key, value):
        self.data[key] = value

    def get_data(self, key):
        return self.data.get(key)

    def remove_data(self, key):
        self.data.pop(key, None)

    # Methods to interact with `lists`
    def add_to_list(self, key, value):
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].append(value)

    def get_list(self, key):
        return self.lists.get(key, [])

    def clear_list(self, key):
        if key in self.lists:
            self.lists[key] = []

    # Methods to interact with `expiry`
    def set_expiry(self, key, value):
        self.expiry[key] = value

    def get_expiry(self, key):
        return self.expiry.get(key)

    # Methods to interact with `counters`
    def increment_counter(self, key, amount=1):
        self.counters[key] = self.counters.get(key, 0) + amount

    def get_counter(self, key):
        return self.counters.get(key, 0)
    
# let's register that to the Manger istances
BaseManager.register('SharedObject', SharedObject)

def worker(shared_obj):
    """Function to modify the shared object."""
    shared_obj.set_data("name", "Alice")
    shared_obj.add_to_list("numbers", 42)
    shared_obj.increment_counter("visits")

if __name__ == "__main__":
    with BaseManager() as manager:
        shared = manager.SharedObject()  # Get the shared proxy object

        p1 = Process(target=worker, args=(shared,))
        p2 = Process(target=worker, args=(shared,))

        p1.start()
        p2.start()
        p1.join()
        p2.join()

        # Print final values in main process
        print(f"Name: {shared.get_data('name')}")
        print(f"Numbers List: {shared.get_list('numbers')}")
        print(f"Visits Counter: {shared.get_counter('visits')}")


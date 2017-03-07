"""
Utility functions.
"""

from contextlib import contextmanager
import warnings
import os

# Python 2/3 independant dict iteration
iteritems = getattr(dict, 'iteritems', dict.items)
itervalues = getattr(dict, 'itervalues', dict.values)



class LRUCache(dict):
    """
    A simple LRU cache.
    """

    def __init__(self, *args, **kwargs):
        self.capacity = kwargs.pop('capacity', None) or float('nan')
        self.lru = []

        super(LRUCache, self).__init__(*args, **kwargs)

    def refresh(self, key):
        if key in self.lru:
            self.lru.remove(key)
        self.lru.append(key)

    def get(self, key, default=None):
        item = super(LRUCache, self).get(key, default)
        self.refresh(key)

        return item

    def __getitem__(self, key):
        item = super(LRUCache, self).__getitem__(key)
        self.refresh(key)

        return item

    def __setitem__(self, key, value):
        super(LRUCache, self).__setitem__(key, value)

        self.refresh(key)

        if len(self) > self.capacity:
            self.pop(self.lru.pop(0))

    def __delitem__(self, key):
        super(LRUCache, self).__delitem__(key)
        self.lru.remove(key)

    def clear(self):
        super(LRUCache, self).clear()
        del self.lru[:]
        

def touch(fname, times=None, create_dirs=False):
    if create_dirs:
        base_dir = os.path.dirname(fname)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
    with open(fname, 'a'):
        os.utime(fname, times)


@contextmanager
def catch_warning(warning_cls):
    with warnings.catch_warnings():
        warnings.filterwarnings('error', category=warning_cls)

        yield


 
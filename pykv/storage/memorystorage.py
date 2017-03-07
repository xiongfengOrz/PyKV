# -*- coding: utf-8 -*-

from pykv.storage.base import Storage


class MemoryStorage(Storage):
    def __init__(self):
        super(MemoryStorage, self).__init__()
        self.memory = None

    def read(self):
        return self.memory

    def write(self, data):
        self.memory = data

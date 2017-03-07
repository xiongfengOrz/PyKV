# -*- coding: utf-8 -*-
import os
from pykv.utils import touch
from pykv.storage.base import Storage


try:
    import ujson as json
except ImportError:
    import json



class JSONStorage(Storage):
    def __init__(self, path, create_dirs=False, **kwargs):
        super(JSONStorage, self).__init__()
        touch(path, create_dirs=create_dirs)  
        self.kwargs = kwargs
        self._handle = open(path, 'r+')

    def close(self):
        self._handle.close()

    def read(self):
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if not size:
            return None
        else:
            self._handle.seek(0)
            return json.load(self._handle)

    def write(self, data):
        self._handle.seek(0)
        serialized = json.dumps(data, **self.kwargs)
        self._handle.write(serialized)
        self._handle.flush()
        self._handle.truncate()


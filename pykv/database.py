
from pykv import JSONStorage
from pykv.utils import LRUCache, iteritems, itervalues


class Element(dict):
    def __init__(self, value=None, eid=None, **kwargs):
        super(Element, self).__init__(**kwargs)

        if value is not None:
            self.update(value)
            self.eid = eid



class TinyDB(object):
    """
    DB main class
    """
    def __init__(self,  *args, **kwargs):
        storage = kwargs.pop('storage', JSONStorage)
        cache_size = kwargs.pop('cache_size', 10)
        
        self._opened = False
        self._storage = storage(*args, **kwargs) 
        self._opened = True 
     
        self._query_cache = LRUCache(capacity=cache_size)
        data = self._read()
        if data:
            self._last_id = max(i for i in data)
        else:
            self._last_id = 0
            
    def close(self):
        self._opened = False
        self._storage.close() 
        
    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._opened is True:
            self.close()

    def process_elements(self, func, cond=None, eids=None):
        data = self._read()

        if eids is not None:
            for eid in eids:
                func(data, eid)

        else:
            eids = []
            for eid in list(data):
                if cond(data[eid]):
                    func(data, eid)
                    eids.append(eid)

        self._write(data)

        return eids

    def clear_cache(self):
        self._query_cache.clear()

    def _get_next_id(self):
        current_id = self._last_id + 1
        self._last_id = current_id

        return current_id

    def _read(self):
        try:
            raw_data = (self._storage.read() or {})
        except KeyError:
            self.write({})
            return {}
        
        data = {}
        for key, val in iteritems(raw_data):
            eid = int(key)
            data[eid] = Element(val, eid)
        
        return data
        
    def _write(self, values):
        self._query_cache.clear()
        self._storage.write(values)

    def __len__(self):
        return len(self._read())

    def all(self):
        return list(itervalues(self._read()))

    def insert(self, elements):
        eid = []
        for element in elements:
            eid.append(self._get_next_id())
    
            if not isinstance(element, dict):
                raise ValueError('Element is not a dictionary')
    
            data = self._read()
            data[eid[-1]] = element
            self._write(data)

        return eid

    def insert_multiple(self, elements):
        eids = []
        data = self._read()

        for element in elements:
            eid = self._get_next_id()
            eids.append(eid)

            data[eid] = element

        self._write(data)

        return eids

    def remove(self, cond=None, eids=None):
        return self.process_elements(lambda data, eid: data.pop(eid),
                                     cond, eids)

    def update(self, fields, cond=None, eids=None):
        if callable(fields):
            return self.process_elements(
                lambda data, eid: fields(data, eid),
                cond, eids
            )
        else:
            return self.process_elements(
                lambda data, eid: data[eid].update(fields),
                cond, eids
            )

    def purge(self):
        self._write({})
        self._last_id = 0

    def search(self, cond):
        if cond in self._query_cache:
            return self._query_cache[cond]

        elements = [element for element in self.all() if cond(element)]
        self._query_cache[cond] = elements

        return elements

    def get(self, cond=None, eid=None):
        if eid is not None:
            return self._read().get(eid, None)

        for element in self.all():
            if cond(element):
                return element

    def count(self, cond):
        return len(self.search(cond))

    def contains(self, cond=None, eids=None):
        if eids is not None:
            return any(self.get(eid=eid) for eid in eids)
        return self.get(cond) is not None



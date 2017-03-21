"""
Contains the querying interface.

"""

import re
import sys
import functools

from pykv.utils import catch_warning 

__all__ = ('Query', 'where', 'QueryOps', 'QueryImpl')


def is_sequence(obj):
    return hasattr(obj, '__iter__')


class QueryOps(object):
    """
    Sever-end operation class
    """
    def delete(self, data, eid):
        data.pop(eid)
        return


    def increment(self, field):
        pass

    def decrement(self, field):
        pass
        
    
class QueryInfo(object):
    """
    Used to pass query info from client to server
    """
    def __init__(self, entry=None, op=None, *args, **kwargs):
        """
        :type 
        """
        self.entry = [] if entry is None else entry
        self.op = op
        self.args = args
        self.kwargs = kwargs


class QueryImpl(object):
    """
    A query implementation.
    """
    def __init__(self, test, hashval):
        self.test = test
        self.hashval = hashval

    def __call__(self, value):
        return self.test(value)

    def __hash__(self):
        return hash(self.hashval)

    def __repr__(self):
        return 'QueryImpl{0}'.format(self.hashval)

    def __eq__(self, other):
        return self.hashval == other.hashval
    
    def __and__(self, other):
        return QueryImpl(lambda value: self(value) and other(value),
                         ('and', frozenset([self.hashval, other.hashval])))

    def __or__(self, other):
        return QueryImpl(lambda value: self(value) or other(value),
                         ('or', frozenset([self.hashval, other.hashval])))

    def __invert__(self):
        return QueryImpl(lambda value: not self(value),
                         ('not', self.hashval))


class Query(object):
    """
    TinyDB Queries.
    """

    def __init__(self, path = None):
        self._path = path if not path == None else []

    def __getattr__(self, item):
        query = Query()
        query._path = self._path + [item]

        return query

    __getitem__ = __getattr__

    def _generate_test(self, test, hashval):
        if not self._path:
            raise ValueError('Query has no path')

        def impl(value):
            try:
                for part in self._path:
                    value = value[part]
            except (KeyError, TypeError):
                return False
            else:
                return test(value)

        return QueryImpl(impl, hashval)

    
    def _cond(self, test):  # is this ok???
        def warpper(value): # only support one arg
            try:
                for part in self._path:
                    value = value[part]
            except (KeyError, TypeError):
                return False
            else:
                return test(value) 
        return warpper
        
    def __eq__(self, rhs):
        if sys.version_info <= (3, 0):
            @self._cond
            def cond(value):
                with catch_warning(UnicodeWarning):
                    try:
                        return value == rhs
                    except UnicodeWarning:
                        if isinstance(value, str):
                            return value.decode('utf-8') == rhs
                        elif isinstance(rhs, str):
                            return value == rhs.decode('utf-8')

        else:
            @self._cond
            def cond(value):
                return value == rhs

        return QueryImpl(cond, ('==', tuple(self._path), rhs))
     
    def __ne__(self, rhs):      
        #cond = functools.partial(self._cond, test = lambda value: value != rhs) 
        @self._cond
        def cond(value):
            return value != rhs
        return QueryImpl(cond, ('!=', tuple(self._path), rhs))

    def __lt__(self, rhs):
        return self._generate_test(lambda value: value < rhs,
                                   ('<', tuple(self._path), rhs))

    def __le__(self, rhs):
        return self._generate_test(lambda value: value <= rhs,
                                   ('<=', tuple(self._path), rhs))

    def __gt__(self, rhs):
        return self._generate_test(lambda value: value > rhs,
                                   ('>', tuple(self._path), rhs))

    def __ge__(self, rhs):
        return self._generate_test(lambda value: value >= rhs,
                                   ('>=', tuple(self._path), rhs))

    def exists(self):
        return self._generate_test(lambda _: True,
                                   ('exists', tuple(self._path)))

    def matches(self, regex):
        return self._generate_test(lambda value: re.match(regex, value),
                                   ('matches', tuple(self._path), regex))

    def search(self, regex):
        return self._generate_test(lambda value: re.search(regex, value),
                                   ('search', tuple(self._path), regex))

    def test(self, func, *args):
        return self._generate_test(lambda value: func(value, *args),
                                   ('test', tuple(self._path), func, args))

    '''
    def any(self, cond):
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and any(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and any(e in cond for e in value)

        return self._generate_test(lambda value: _cmp(value),
                                   ('any', tuple(self._path), cond))

    def all(self, cond):
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and all(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and all(e in value for e in cond)

        return self._generate_test(lambda value: _cmp(value),
                                   ('all', tuple(self._path), cond))
     '''
 

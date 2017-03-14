"""
Contains the client querying interface.

Including QueryInfo, Query

"""

import re
import sys

from pykv.utils import catch_warning

__all__ = ('Query', 'QueryInfo')


       
    
class QueryInfo(object):
    """
    Used to pass query info from client to server
    
    """
    def __init__(self, entry=None, op=None, args=None, connection=None, **kwargs):
        """
        :type entry: list
        :type op: list
        :type args: list
        :type connection: list
        """        
        self.entry = [] if entry is None else entry
        self.op = [] if op is None else op
        self.args = [] if args is None else args
        self.connection = [] if connection is None else connection
        self.kwargs = kwargs
        
    def __str__(self):
        return 'QueryInfo{0},{1},{2},{3}'.format(self.entry, self.op, self.args, self.connection)
    
    
    def __and__(self, other):
        self.entry.extend(other.entry)
        self.op.extend(other.op)
        self.args.extend(other.args)
        self.connection.append('__and__')        
        return self
    
    def __or__(self, other):
        self.entry.extend(other.entry)
        self.op.extend(other.op)
        self.args.extend(other.args)
        self.connection.append('__or__')        
        return self
    
    def __invert__(self):
        """
        Just need to add __or__ info
        """
        self.connection.append('__or__')        
        return self

 

class Query(object):
    """
    Client Queries.

    """

    def __init__(self):
        self._path = []

    def __getattr__(self, item):
        query = Query()
        query._path = self._path + [item]

        return query

    __getitem__ = __getattr__

 

    def __eq__(self, rhs):
        if self._path == []:
            return 
        
        return QueryInfo([self._path], ["__eq__"], [rhs])

    def __ne__(self, rhs):
        return QueryInfo([self._path], ["__ne__"], [rhs])


    def __lt__(self, rhs):
        return QueryInfo([self._path], ["__lt__"], [rhs])

    def __le__(self, rhs):
        return QueryInfo([self._path], ["__le__"], [rhs])

    def __gt__(self, rhs):
        return QueryInfo([self._path], ["__gt__"], [rhs])

    def __ge__(self, rhs):
        return QueryInfo([self._path], ["__ge__"], [rhs])

    def exists(self):
        return QueryInfo([self._path], ["exists"], [rhs])

    def matches(self, regex):
        return QueryInfo([self._path], ["matches"], [rhs])

    def search(self, regex):
        return QueryInfo([self._path], ["search"], [rhs])
       
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



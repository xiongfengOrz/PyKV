"""
Contains the client querying interface.

Including QueryInfo, Query

"""

import re
import sys

from pykv.utils import catch_warning

__all__ = ('Query', 'where', 'QueryInfo')


       
    
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
        """
        Test a dict value for equality.

        >>> Query().f1 == 42

        :param rhs: The value to compare against
        """
        if self._path == []:
            return 
        
        return QueryInfo([self._path], ["__eq__"], [rhs])

    def __ne__(self, rhs):
        """
        Test a dict value for inequality.

        >>> Query().f1 != 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["__ne__"], [rhs])


    def __lt__(self, rhs):
        """
        Test a dict value for being lower than another value.

        >>> Query().f1 < 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["__lt__"], [rhs])

    def __le__(self, rhs):
        """
        Test a dict value for being lower than or equal to another value.

        >>> where('f1') <= 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["__le__"], [rhs])

    def __gt__(self, rhs):
        """
        Test a dict value for being greater than another value.

        >>> Query().f1 > 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["__gt__"], [rhs])

    def __ge__(self, rhs):
        """
        Test a dict value for being greater than or equal to another value.

        >>> Query().f1 >= 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["__ge__"], [rhs])

    def exists(self):
        """
        Test for a dict where a provided key exists.

        >>> Query().f1.exists() >= 42

        :param rhs: The value to compare against
        """
        return QueryInfo([self._path], ["exists"], [rhs])

    def matches(self, regex):
        """
        Run a regex test against a dict value (whole string has to match).

        >>> Query().f1.matches(r'^\w+$')

        :param regex: The regular expression to use for matching
        """
        return QueryInfo([self._path], ["matches"], [rhs])

    def search(self, regex):
        """
        Run a regex test against a dict value (only substring string has to
        match).

        >>> Query().f1.search(r'^\w+$')

        :param regex: The regular expression to use for matching
        """
        return QueryInfo([self._path], ["search"], [rhs])

 

    def any(self, cond):
        """
        Checks if a condition is met by any element in a list,
        where a condition can also be a sequence (e.g. list).

        >>> Query().f1.any(Query().f2 == 1)

        Matches::

            {'f1': [{'f2': 1}, {'f2': 0}]}

        >>> Query().f1.any([1, 2, 3])
        # Match f1 that contains any element from [1, 2, 3]

        Matches::

            {'f1': [1, 2]}
            {'f1': [3, 4, 5]}

        :param cond: Either a query that at least one element has to match or
                     a list of which at least one element has to be contained
                     in the tested element.
-       """
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and any(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and any(e in cond for e in value)

        return self._generate_test(lambda value: _cmp(value),
                                   ('any', tuple(self._path), cond))

    def all(self, cond):
        """
        Checks if a condition is met by any element in a list,
        where a condition can also be a sequence (e.g. list).

        >>> Query().f1.all(Query().f2 == 1)

        Matches::

            {'f1': [{'f2': 1}, {'f2': 1}]}

        >>> Query().f1.all([1, 2, 3])
        # Match f1 that contains any element from [1, 2, 3]

        Matches::

            {'f1': [1, 2, 3, 4, 5]}

        :param cond: Either a query that all elements have to match or a list
                     which has to be contained in the tested element.
        """
        if callable(cond):
            def _cmp(value):
                return is_sequence(value) and all(cond(e) for e in value)

        else:
            def _cmp(value):
                return is_sequence(value) and all(e in value for e in cond)

        return self._generate_test(lambda value: _cmp(value),
                                   ('all', tuple(self._path), cond))


def where(key):
    return Query()[key]
 

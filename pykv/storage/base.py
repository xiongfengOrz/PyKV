# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class Storage(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def read(self):
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def write(self, data):
        raise NotImplementedError('To be overridden!')

    def close(self):
        pass



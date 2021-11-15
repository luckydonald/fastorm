#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List, Any

from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if

__all__ = ['In']


class In(object):
    __slots__ = ['variables', '_flattened_cache']

    variables: List[Any]

    def __init__(self, *variables):
        self.variables = [v for v in variables]
        self._flattened_cache = None
    # end def

    def __str__(self):
        return f'{self.__class__.__name__!s}{self.variables!s}'
    # end def

    def __repr__(self):
        return f'{self.__class__.__name__!s}{self.variables!r}'
    # end def

    def __class_getitem__(cls, variables):
        if not isinstance(variables, tuple):
            variables = (variables,)
        # end if
        return cls(*variables)
    # end def

    def __iter__(self):
        """
        Iterate all values, and also flattens other contained `In` instances, too.

        So in other words:

          >>> var = In[1, In[2, 3], 4, In[In[In[5], 6], 7]]
          >>> list(var)
          [1, 2, 3, 4, 5, 6, 7]

          >>> var = In(1, In(2, 3), 4, In(In[In[5), 6), 7)
          >>> list(var)
          [1, 2, 3, 4, 5, 6, 7]

          >>> var = In[In(), In()]
          >>> list(var)
          []

        :return:
        """
        for variable in self.variables:
            if isinstance(variable, In):
                yield from variable
            else:
                yield variable
            # end if
        # end for
    # end for

    def as_list(self) -> List[Any]:
        if self._flattened_cache is None:
            self._flattened_cache = list(self)
        # end if
        return self._flattened_cache
    # end def

    def __len__(self):
        return len(self.as_list())
    # end def

    def flatten(self) -> 'In':
        return In(*self.as_list())
    # end def

    def __eq__(self, other):
        if isinstance(other, In):
            other = other.as_list()
        # end if
        if isinstance(other, list):
            return self.as_list() == other
        # end if
        raise TypeError(f'Could not compare with {type(other)}: {other!r}')
    # end def
# end class

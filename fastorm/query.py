#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import inspect
from typing import List, Any, TypeVar, Generic

from luckydonaldUtils.logger import logging

from .compat import check_is_annotated_type, check_is_union_type, check_is_generic_alias

__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if

__all__ = ['In']

VARIABLE_TYPE = TypeVar("VARIABLE_TYPE", bound=Any)



class In(Generic[VARIABLE_TYPE]):
    __slots__ = ['variables', '_flattened_cache']

    variables: List[VARIABLE_TYPE]

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
        if (
            inspect.isclass(variables) or
            check_is_annotated_type(variables) or
            check_is_union_type(variables) or
            check_is_generic_alias(variables)
        ):
            # keep type hint support
            return super().__class_getitem__(variables)
        # end if
        if not isinstance(variables, tuple):
            variables = (variables,)
        # end if
        return cls(*variables)
    # end def

    def __iter__(self):
        """
        Iterate all values, and also flattens other contained `In` instances, too.

        So in other words:

          >>> var1 = In[1, In[2, 3], 4, In[In[In[5], 6], 7]]
          >>> list(var1)
          [1, 2, 3, 4, 5, 6, 7]

          >>> var2 = In(1, In(2, 3), 4, In(In(In(5), 6), 7))
          >>> list(var2)
          [1, 2, 3, 4, 5, 6, 7]

          >>> var1 == var1
          True

          >>> var1 == var2
          True

          >>> var3 = In[In(), In()]
          >>> list(var3)
          []

          >>> In[In(), In()] == []
          True
          >>> In[In(), In()] == In(1, In(2, 3), 4, In(In(In(5), 6), 7))
          False

          >>> In[int]
          fastorm.query.In[int]

          >>> class InGeneric(Generic[VARIABLE_TYPE]):
          ...   pass

          >>> InGeneric[int]
          fastorm.query.InGeneric[int]

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
            self._flattened_cache = list(var for var in self)
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
        if len(self.as_list()) == 1:
            return self.as_list()[0] == other
        # end if

        raise TypeError(f'Could not compare with {type(other)}: {other!r}')
    # end def
# end class

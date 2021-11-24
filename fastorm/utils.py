#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import ForwardRef

from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if


def failsafe_issubclass(var, types):
    try:
        return issubclass(var, types)
    except TypeError:
        return False
    # end try
# end def


def evaluate_forward_ref(cls, key, potential_forward_ref):
    if failsafe_isinstance(potential_forward_ref, ForwardRef):
        if not potential_forward_ref.__forward_evaluated__:
            raise ValueError(
                f'The typehint of {cls.__name__}.{key} is still a unresolved ForwardRef. You should probably call {cls.__name__}.update_forward_refs() after the class it is pointing to is defined.'
            )
        # end if
        return potential_forward_ref.__forward_value__
    # end if
    return potential_forward_ref
# end def


def failsafe_isinstance(var, types):
    try:
        return isinstance(var, types)
    except TypeError:
        return False
    # end try
# end def



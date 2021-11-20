#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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


def failsafe_isinstance(var, types):
    try:
        return isinstance(var, types)
    except TypeError:
        return False
    # end try
# end def



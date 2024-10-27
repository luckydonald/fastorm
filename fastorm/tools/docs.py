#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if

from typing import Callable, TypeVar, Union


FUNCTION_TO_DOCUMENT = TypeVar('FUNCTION_TO_DOCUMENT', bound=Callable)


def append_to_docs(docs_to_append: str) -> Callable[[FUNCTION_TO_DOCUMENT], FUNCTION_TO_DOCUMENT]:
    def append_to_docs_function_inner(func: FUNCTION_TO_DOCUMENT) -> FUNCTION_TO_DOCUMENT:
        func.__doc__ += "\n" + docs_to_append
        return func
    # end def
    return append_to_docs_function_inner
# end def

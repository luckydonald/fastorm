#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import ForwardRef, Optional
import re

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


def evaluate_forward_ref(potential_forward_ref, key: Optional[str], cls_name: Optional[str]):
    if failsafe_isinstance(potential_forward_ref, ForwardRef):
        if not potential_forward_ref.__forward_evaluated__:
            raise ValueError(
                f'The typehint of {cls_name}.{key} is still a unresolved ForwardRef. You should probably call {cls_name}.update_forward_refs() after the class it is pointing to is defined.'
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


def snakecase(string: str) -> str:
    """Convert string into snake case.
    Join punctuation with underscore
    Args:
        string: String to convert.
    Returns:
        string: Snake cased string.
    """
    # based on https://github.com/okunishinishi/python-stringcase/blob/04afe0044a9c513bc7899ae2dd1b97f1fe165f6c/stringcase.py#L141-L156

    # noinspection RegExpRedundantEscape
    string = re.sub(r"[^\w\d]", '_', str(string))
    if not string:
        return string
    # end if
    string = string[0].lower() + re.sub(r"[A-Z]", lambda matched: '_' + matched.group(0).lower(), string[1:])
    string = re.sub(r"(\D)(\d)", lambda matched: matched.group(1) + '_' + matched.group(2), string)
    return re.sub(r"_+", '_', string)
# end def


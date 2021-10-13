#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional

from luckydonaldUtils.logger import logging
from luckydonaldUtils.typing import JSONType


__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if


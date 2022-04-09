#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from luckydonaldUtils.logger import logging, DEFAULT_DATE_FORMAT
import time

__author__ = 'luckydonald'

from tests.tools_for_the_tests_of_fastorm.TestRunner import main

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG, date_formatter='')
# end if


if __name__ == "__main__":
    logger.debug(f'Tests started at {time.strftime(DEFAULT_DATE_FORMAT)}.')
    main(module=None)



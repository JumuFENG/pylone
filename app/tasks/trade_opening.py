# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
sys.path.insert(0, os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/../..'))

from app.lofig import Config, logging
from app.stock.manager import AllBlocks
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


async def bk_changes_prepare_task():
    bks = await AllBlocks.bkchanges.dumpDataByDate()
    logger.info('em bks to load %d: %s', len(bks), bks)
    clsbks = await AllBlocks.clsbkchanges.dumpDataByDate()
    for bk in bks + clsbks:
        await AllBlocks.load_info(bk)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bk_changes_prepare_task())

#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch, AsyncMock
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
from app.stock.manager import AllBlocks
from app.selectors import *

class TestSelector(unittest.IsolatedAsyncioTestCase):
    async def test_ztlead_selector(self):
        sz = StockZtLeadingSelector()
        bkstocks = await AllBlocks.bk_stocks('cls82517,cls80222,cls80454'.split(','))
        x = await sz.getHeadedStocks(bkstocks, '2025-12-24')
        self.assertIsInstance(x, list)


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestSelector('test_ztlead_selector'))
    unittest.TextTestRunner().run(suite)

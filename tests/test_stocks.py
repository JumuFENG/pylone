#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch
import numpy as np
import json
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
from app.stock.manager import AllStocks, AllIndexes

class TestIndex(unittest.IsolatedAsyncioTestCase):
    async def test_load_index(self):
        await AllIndexes.load_info('sh000001')

    async def test_update_kline_data(self):
        await AllIndexes.update_kline_data()

    async def test_np_convert(self):
        from app.stock.h5 import KLineStorage
        kls = KLineStorage(2, 0)
        arr = [['2023-10-01 00:00', 1.00, 1.05, .95, 1.02, 1000],
               ['2023-10-02 15:00', 1.02, 1.08, 1.00, 1.07, 1200],
               ['2023-10-03 09:30', 1.07, 1.10, 1.05, 1.09, 1500],
               ['2023-10-04 11:30:30', 1.09, 1.15, 1.08, 1.13, 1300],
               ['2023-10-05', 1.13, 1.20, 1.10, 1.18, 1400]]
        dtypes = [('time', 'U20'), ('open', 'float64'), ('high', 'float64'), ('low', 'float64'), ('close', 'float64'), ('volume', 'int64')]
        odata = kls.prepare_data(np.array([tuple(r) for r in arr], dtype=dtypes))
        print(odata.dtype)
        rdata = kls.restore_data(odata)
        print(rdata.dtype)

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestIndex('test_update_kline_data'))
    unittest.TextTestRunner().run(suite)

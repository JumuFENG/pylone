#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch
import json
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
from app.stock.h5 import explore_hdf5, read_kline

class TestH5file(unittest.TestCase):
    def test_read_structure(self):
        explore_hdf5('./data/bj_day.h5')

    def test_get_kline(self):
        kline = read_kline('BJ920950', 'day', length=30)
        print(kline)
        self.assertIsNotNone(kline)

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestH5file('test_get_kline'))
    unittest.TextTestRunner().run(suite)

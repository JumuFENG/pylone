#!/usr/bin/env python3
import unittest
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.stock.quotes import Quotes

class TestQuotes(unittest.TestCase):

    def test_bar_time_no_bar(self):
        result = Quotes._bar_time('2025-12-19 09:15', 5)
        self.assertEqual(result, '2025-12-19 09:35')

    def test_bar_time_time_greater_than_bar(self):
        result = Quotes._bar_time( '09:25', 5)
        self.assertEqual(result, '09:35')

    def test_bar_time_time_equal_to_bar(self):
        result = Quotes._bar_time('09:34', 5)
        self.assertEqual(result, '09:35')
        result = Quotes._bar_time('2025-12-19 09:30', 1)
        self.assertEqual(result, '2025-12-19 09:31')

    def test_bar_time_time_before_bar(self):
        result = Quotes._bar_time('2025-12-19 10:15', 5)
        self.assertEqual(result, '2025-12-19 10:20')

    def test_bar_time_crossing_hour_boundary(self):
        result = Quotes._bar_time('2025-12-19 11:30', 5)
        self.assertEqual(result, '2025-12-19 11:30')

if __name__ == '__main__':
    unittest.main()
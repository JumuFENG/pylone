#!/usr/bin/env python3
import unittest
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.stock.quotes import Quotes

class TestQuotes(unittest.TestCase):

    def test_next_bar_time_no_bar(self):
        result = Quotes._next_bar_time(None, '2025-12-19 09:15', 5)
        self.assertEqual(result, '09:35')

    def test_next_bar_time_time_greater_than_bar(self):
        result = Quotes._next_bar_time({'time': '2025-12-19 09:10'}, '2025-12-19 09:15', 5)
        self.assertEqual(result, '09:35')

    def test_next_bar_time_time_equal_to_bar(self):
        result = Quotes._next_bar_time({'time': '2025-12-19 09:15'}, '2025-12-19 09:15', 5)
        self.assertEqual(result, '09:35')

    def test_next_bar_time_time_equal_to_bar(self):
        result = Quotes._next_bar_time({'time': '2025-12-19 09:31'}, '2025-12-19 09:30', 1)
        self.assertIsNone(result)

    def test_next_bar_time_time_before_bar(self):
        result = Quotes._next_bar_time({'time': '2025-12-19 10:20'}, '2025-12-19 10:15', 5)
        self.assertIsNone(result)

    def test_next_bar_time_crossing_hour_boundary(self):
        result = Quotes._next_bar_time({'time': '2025-12-19 11:30'}, '2025-12-19 13:00', 5)
        self.assertEqual(result, '13:05')

if __name__ == '__main__':
    unittest.main()
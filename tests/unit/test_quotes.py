#!/usr/bin/env python3
"""
Unit tests for quotes calculation and management.
"""

import os, sys
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

import unittest
from base import BaseTestCase


class TestQuotes(BaseTestCase):
    """Test quotes calculation utilities."""

    def test_bar_time_no_bar(self):
        """Test _bar_time when time is not aligned with bar."""
        from app.stock.quotes import Quotes
        result = Quotes._bar_time('2025-12-19 09:15', 5)
        self.assertEqual(result, '2025-12-19 09:35')

    def test_bar_time_time_greater_than_bar(self):
        """Test _bar_time when time is greater than bar boundary."""
        from app.stock.quotes import Quotes
        result = Quotes._bar_time('09:25', 5)
        self.assertEqual(result, '09:35')

    def test_bar_time_time_equal_to_bar(self):
        """Test _bar_time when time equals bar boundary."""
        from app.stock.quotes import Quotes

        result = Quotes._bar_time('09:34', 5)
        self.assertEqual(result, '09:35')

        result = Quotes._bar_time('2025-12-19 09:30', 1)
        self.assertEqual(result, '2025-12-19 09:31')

    def test_bar_time_time_before_bar(self):
        """Test _bar_time when time is before bar boundary."""
        from app.stock.quotes import Quotes
        result = Quotes._bar_time('2025-12-19 10:15', 5)
        self.assertEqual(result, '2025-12-19 10:20')

    def test_bar_time_crossing_hour_boundary(self):
        """Test _bar_time when crossing hour boundary."""
        from app.stock.quotes import Quotes
        result = Quotes._bar_time('2025-12-19 11:30', 5)
        self.assertEqual(result, '2025-12-19 11:30')

    def test_bar_time_various_intervals(self):
        """Test _bar_time with various intervals."""
        from app.stock.quotes import Quotes

        # Test 1-minute interval
        result = Quotes._bar_time('09:35:30', 1)
        self.assertEqual(result, '09:36')

        # Test 15-minute interval
        result = Quotes._bar_time('09:45', 15)
        self.assertEqual(result, '10:00')

        # Test 30-minute interval
        result = Quotes._bar_time('10:25', 30)
        self.assertEqual(result, '10:30')

        # Test 60-minute interval
        result = Quotes._bar_time('10:59', 60)
        self.assertEqual(result, '11:30')

    def test_bar_time_edge_cases(self):
        """Test _bar_time with edge cases."""
        from app.stock.quotes import Quotes

        # Test midnight crossover
        result = Quotes._bar_time('23:58', 5)
        self.assertEqual(result, '15:00')  # Should not cross to next day

        # Test exact alignment
        result = Quotes._bar_time('10:00', 5)
        self.assertEqual(result, '10:05')

        result = Quotes._bar_time('11:30', 5)
        self.assertEqual(result, '11:30')

        # Test just after alignment
        result = Quotes._bar_time('10:01', 5)
        self.assertEqual(result, '10:05')


if __name__ == '__main__':
    unittest.main()
#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch
import json
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config

class TestConfig(unittest.TestCase):
    def test_decrypt(self):
        self.assertEqual('abcd123', Config.simple_decrypt('abcd123'))

if __name__ == '__main__':
    unittest.main()
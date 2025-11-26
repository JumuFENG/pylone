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

class TestBearerToken(unittest.TestCase):
    def test_request_token(self):
        import requests
        user = 'admin'
        email = 'admin@admin.com'
        pwd = 'admin123'
        auth_response = requests.post(
            f"http://localhost:{Config.client_config()['port']}/auth/bearer/login",
            data={"username": user, "password": pwd}) 

        token = auth_response.json()["access_token"]
        user_response = requests.get(
            f"http://localhost:{Config.client_config()['port']}/users/me",
            headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(user_response.json()["username"], user)

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestBearerToken('test_request_token'))
    unittest.TextTestRunner().run(suite)

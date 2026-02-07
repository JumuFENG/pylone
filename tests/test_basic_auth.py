"""
测试 Basic Auth 认证
"""
import base64
import os
import sys
import unittest
from typing import Optional

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config

BASE_URL = f"http://localhost:{Config.client_config()['port']}"


def _check_server_availability() -> bool:
    """检查服务器是否运行"""
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        return response.status_code in (200, 404)  # 404 也表示服务器在运行
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return False


class TestBasicAuth(unittest.TestCase):
    """测试 Basic Auth 认证的集成测试类"""

    @classmethod
    def setUpClass(cls):
        """检查服务器是否可用"""
        cls.server_available = _check_server_availability()

    def _get_user_info(self, auth: Optional[tuple] = None, headers: Optional[dict] = None) -> requests.Response:
        """获取用户信息的辅助方法"""
        kwargs = {}
        if auth:
            kwargs['auth'] = auth
        if headers:
            kwargs['headers'] = headers
        return requests.get(f"{BASE_URL}/users/me", **kwargs)

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_basic_auth_email(self):
        """测试使用邮箱 + Basic Auth 认证"""
        response = self._get_user_info(auth=("admin@admin.com", "admin123"))
        
        self.assertEqual(response.status_code, 200)
        user_data = response.json()
        self.assertIn('email', user_data)
        self.assertEqual(user_data['email'], 'admin@admin.com')

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_basic_auth_username(self):
        """测试使用用户名 + Basic Auth 认证"""
        response = self._get_user_info(auth=("admin", "admin123"))
        
        self.assertEqual(response.status_code, 200)
        user_data = response.json()
        self.assertIn('email', user_data)
        self.assertEqual(user_data['email'], 'admin@admin.com')

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_basic_auth_manual_header(self):
        """测试手动构造 Basic Auth header"""
        credentials = base64.b64encode(b"admin@admin.com:admin123").decode('utf-8')
        headers = {"Authorization": f"Basic {credentials}"}
        response = self._get_user_info(headers=headers)
        
        self.assertEqual(response.status_code, 200)
        user_data = response.json()
        self.assertIn('email', user_data)
        self.assertEqual(user_data['email'], 'admin@admin.com')

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_basic_auth_wrong_password(self):
        """测试错误的密码"""
        response = self._get_user_info(auth=("admin@admin.com", "wrongpassword"))
        
        self.assertNotEqual(response.status_code, 200)
        self.assertIn('detail', response.json())

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_bearer_token_auth(self):
        """测试 Bearer Token 认证"""
        # 登录获取 token
        login_response = requests.post(
            f"{BASE_URL}/auth/bearer/login",
            data={"username": "admin@admin.com", "password": "admin123"}
        )
        
        self.assertEqual(login_response.status_code, 200)
        token_data = login_response.json()
        self.assertIn('access_token', token_data)
        
        # 使用 token 访问受保护的端点
        token = token_data["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        response = self._get_user_info(headers=headers)
        
        self.assertEqual(response.status_code, 200)
        user_data = response.json()
        self.assertIn('email', user_data)
        self.assertEqual(user_data['email'], 'admin@admin.com')

    @unittest.skipIf(not _check_server_availability(), "Server not running")
    def test_cookie_auth(self):
        """测试 Cookie 认证"""
        session = requests.Session()
        
        # 登录获取 cookie
        login_response = session.post(
            f"{BASE_URL}/auth/jwt/login",
            data={"username": "admin@admin.com", "password": "admin123"}
        )
        
        self.assertIn(login_response.status_code, (200, 204))
        self.assertIn('fastapiusersauth', session.cookies)
        
        # 使用 cookie 访问受保护的端点
        response = session.get(f"{BASE_URL}/users/me")
        self.assertEqual(response.status_code, 200)
        user_data = response.json()
        self.assertIn('email', user_data)
        self.assertEqual(user_data['email'], 'admin@admin.com')
        
        # 测试自动刷新功能
        refresh_response = session.get(f"{BASE_URL}/users/me?auto_refresh=1")
        self.assertEqual(refresh_response.status_code, 200)
        refresh_data = refresh_response.json()
        self.assertIn('email', refresh_data)
        self.assertEqual(refresh_data['email'], 'admin@admin.com')


if __name__ == "__main__":
    unittest.main()

"""
测试 Basic Auth 认证
"""
import requests
import base64
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
BASE_URL = f"http://localhost:{Config.client_config()['port']}"
# 添加项目路径

def test_basic_auth():
    """测试 Basic Auth 认证"""
    
    # 测试用例 1: 使用邮箱登录
    print("测试 1: 使用邮箱 + Basic Auth")
    response = requests.get(
        f"{BASE_URL}/users/me",
        auth=("admin@admin.com", "admin123")
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"用户信息: {response.json()}")
    else:
        print(f"错误: {response.text}")
    print()
    
    # 测试用例 2: 使用用户名登录
    print("测试 2: 使用用户名 + Basic Auth")
    response = requests.get(
        f"{BASE_URL}/users/me",
        auth=("admin", "admin123")
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"用户信息: {response.json()}")
    else:
        print(f"错误: {response.text}")
    print()
    
    # 测试用例 3: 手动构造 Basic Auth header
    print("测试 3: 手动构造 Basic Auth header")
    credentials = base64.b64encode(b"admin@admin.com:admin123").decode('utf-8')
    response = requests.get(
        f"{BASE_URL}/users/me",
        headers={"Authorization": f"Basic {credentials}"}
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"用户信息: {response.json()}")
    else:
        print(f"错误: {response.text}")
    print()
    
    # 测试用例 4: 错误的密码
    print("测试 4: 错误的密码")
    response = requests.get(
        f"{BASE_URL}/users/me",
        auth=("admin@admin.com", "wrongpassword")
    )
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.text}")
    print()
    
    # 测试用例 5: 对比 Bearer Token 认证
    print("测试 5: Bearer Token 认证（对比）")
    login_response = requests.post(
        f"{BASE_URL}/auth/bearer/login",
        data={"username": "admin@admin.com", "password": "admin123"}
    )
    if login_response.status_code == 200:
        token = login_response.json()["access_token"]
        response = requests.get(
            f"{BASE_URL}/users/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        print(f"状态码: {response.status_code}")
        if response.status_code == 200:
            print(f"用户信息: {response.json()}")
    else:
        print(f"Bearer Token 登录失败: {login_response.status_code}")
        print(f"响应: {login_response.text}")
    print()
    
    # 测试用例 6: Cookie 认证
    print("测试 6: Cookie 认证")
    # 创建一个 session 来自动管理 cookies
    session = requests.Session()
    
    # 6.1 登录获取 cookie
    print("  6.1 登录获取 cookie...")
    login_response = session.post(
        f"{BASE_URL}/auth/jwt/login",
        data={"username": "admin@admin.com", "password": "admin123"}
    )
    print(f"  登录状态码: {login_response.status_code}")
    
    if login_response.status_code == 204 or login_response.status_code == 200:
        # 检查是否设置了 cookie
        if 'fastapiusersauth' in session.cookies:
            print(f"  Cookie 已设置: fastapiusersauth")
            
            # 6.2 使用 cookie 访问受保护的端点
            print("  6.2 使用 cookie 访问 /users/me...")
            response = session.get(f"{BASE_URL}/users/me")
            print(f"  状态码: {response.status_code}")
            if response.status_code == 200:
                print(f"  用户信息: {response.json()}")
            else:
                print(f"  错误: {response.text}")
        else:
            print(f"  警告: Cookie 未设置")
            print(f"  可用的 cookies: {session.cookies.get_dict()}")
    else:
        print(f"  Cookie 登录失败: {login_response.status_code}")
        print(f"  响应: {login_response.text}")
    print()

if __name__ == "__main__":
    print("=" * 60)
    print("Basic Auth 认证测试")
    print("=" * 60)
    print()
    test_basic_auth()
    print("=" * 60)
    print("测试完成")
    print("=" * 60)

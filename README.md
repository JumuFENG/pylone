# FastAPI 用户管理系统

基于 FastAPI 和 FastAPI Users 实现的用户管理系统，使用 MySQL 数据库。

## 功能特性

- 用户注册
- 用户登录（JWT 认证）
- 用户信息管理
- 受保护的路由
- 异步数据库操作

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置数据库

编辑 `config/config.json` 文件，配置你的 MySQL 数据库信息：

```json
{
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "your_password",
        "database": "user_management"
    }
}
```

## 创建数据库

在 MySQL 中创建数据库：

```sql
CREATE DATABASE user_management CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## 运行应用

```bash
python main.py
```

或者使用 uvicorn：

```bash
uvicorn main:app --reload
```

应用将在 http://localhost:8000 启动

## API 文档

启动应用后，访问以下地址查看 API 文档：

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API 端点

### 认证相关

- `POST /auth/register` - 注册新用户
- `POST /auth/jwt/login` - 登录获取 JWT token
- `POST /auth/jwt/logout` - 登出

### 用户管理

- `GET /users/me` - 获取当前用户信息
- `PATCH /users/me` - 更新当前用户信息
- `GET /users/{id}` - 获取指定用户信息（需要超级用户权限）
- `PATCH /users/{id}` - 更新指定用户信息（需要超级用户权限）
- `DELETE /users/{id}` - 删除用户（需要超级用户权限）

### 示例

- `GET /protected-route` - 受保护的路由示例

## 网页端使用

启动应用后，访问 http://localhost:8000 即可使用网页界面：

### 功能页面

- **首页**: http://localhost:8000/
- **注册页面**: http://localhost:8000/register.html
- **登录页面**: http://localhost:8000/login.html
- **个人信息页**: http://localhost:8000/profile.html
- **用户管理页** (管理员): http://localhost:8000/admin.html

### 网页功能

1. **用户注册**: 填写用户名、邮箱和密码即可注册
2. **用户登录**: 使用邮箱和密码登录
3. **个人信息**: 查看和更新个人信息（用户名、密码）
4. **用户管理** (仅管理员):
   - 查看所有用户列表
   - 启用/禁用用户
   - 设置/取消管理员权限
   - 删除用户

## API 使用示例

### 注册用户

```bash
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "password123",
    "username": "testuser"
  }'
```

### 登录

```bash
curl -X POST "http://localhost:8000/auth/jwt/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=user@example.com&password=password123"
```

### 访问受保护路由

```bash
curl -X GET "http://localhost:8000/protected-route" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

## 创建管理员账号

首次使用时，需要手动在数据库中将某个用户设置为管理员：

```sql
UPDATE user SET is_superuser = 1 WHERE email = 'admin@example.com';
```

或者在注册后通过 API 更新（需要数据库直接操作）。

## 安全提示

⚠️ 在生产环境中，请务必：
1. 修改 `app/users.py` 中的 `SECRET` 密钥
2. 使用 HTTPS
3. 配置 CORS 策略
4. 设置强密码策略

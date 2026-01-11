const API_BASE = '';


function showError(message) {
    const alertDiv = document.getElementById('alert');
    if (alertDiv) {
        alertDiv.className = 'alert alert-error';
        alertDiv.textContent = message;
        alertDiv.style.display = 'block';
        setTimeout(() => {
            alertDiv.style.display = 'none';
        }, 3000);
    }
}

function showSuccess(message) {
    const alertDiv = document.getElementById('alert');
    if (alertDiv) {
        alertDiv.className = 'alert alert-success';
        alertDiv.textContent = message;
        alertDiv.style.display = 'block';
        setTimeout(() => {
            alertDiv.style.display = 'none';
        }, 3000);
    }
}

async function register(userData) {
    try {
        const response = await fetch(`${API_BASE}/auth/register`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            credentials: 'include',
            body: JSON.stringify(userData)
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || '注册失败');
        }

        return await response.json();
    } catch (error) {
        throw error;
    }
}

async function login(email, password) {
    try {
        const formData = new URLSearchParams();
        formData.append('username', email);
        formData.append('password', password);

        const response = await fetch(`${API_BASE}/auth/jwt/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            credentials: 'include',
            body: formData
        });

        if (!response.ok) {
            throw new Error('登录失败：邮箱或密码错误');
        }

        // Cookie 认证返回 204，不需要处理响应体
        return { success: true };
    } catch (error) {
        throw error;
    }
}

async function logout() {
    try {
        await fetch(`${API_BASE}/auth/jwt/logout`, {
            method: 'POST',
            credentials: 'include'
        });
    } catch (error) {
        console.error('Logout error:', error);
    } finally {
        window.location.href = '/html/login.html';
    }
}

async function getCurrentUser() {
    const response = await fetch(`${API_BASE}/users/me?auto_refresh=1`, {
        credentials: 'include'
    });

    if (!response.ok) {
        if (response.status === 401) {
            window.location.href = '/html/login.html';
        }
        throw new Error('获取用户信息失败');
    }

    return await response.json();
}

async function updateUser(userId, data) {
    const response = await fetch(`${API_BASE}/users/${userId}`, {
        method: 'PATCH',
        headers: {
            'Content-Type': 'application/json'
        },
        credentials: 'include',
        body: JSON.stringify(data)
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || '更新失败');
    }

    return await response.json();
}

async function getAllUsers() {
    const response = await fetch(`${API_BASE}/admin/users`, {
        credentials: 'include'
    });

    if (!response.ok) {
        throw new Error('获取用户列表失败');
    }

    return await response.json();
}

async function deleteUser(userId) {
    const response = await fetch(`${API_BASE}/users/${userId}`, {
        method: 'DELETE',
        credentials: 'include'
    });

    if (!response.ok) {
        throw new Error('删除用户失败');
    }
}

async function checkAuth() {
    try {
        const response = await fetch(`${API_BASE}/users/me`, {
            credentials: 'include'
        });
        if (!response.ok) {
            window.location.href = '/html/login.html';
        }
    } catch (error) {
        window.location.href = '/html/login.html';
    }
}

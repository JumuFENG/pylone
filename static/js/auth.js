const API_BASE = '';

function getToken() {
    return localStorage.getItem('access_token');
}

function setToken(token) {
    localStorage.setItem('access_token', token);
}

function clearToken() {
    localStorage.removeItem('access_token');
}

function showError(message) {
    const alertDiv = document.getElementById('alert');
    if (alertDiv) {
        alertDiv.className = 'alert alert-error';
        alertDiv.textContent = message;
        alertDiv.style.display = 'block';
    }
}

function showSuccess(message) {
    const alertDiv = document.getElementById('alert');
    if (alertDiv) {
        alertDiv.className = 'alert alert-success';
        alertDiv.textContent = message;
        alertDiv.style.display = 'block';
    }
}

async function register(email, password, username) {
    try {
        const response = await fetch(`${API_BASE}/auth/register`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                email: email,
                password: password,
                username: username
            })
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
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || '登录失败');
        }

        const data = await response.json();
        setToken(data.access_token);
        return data;
    } catch (error) {
        throw error;
    }
}

async function logout() {
    try {
        const token = getToken();
        if (token) {
            await fetch(`${API_BASE}/auth/jwt/logout`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });
        }
    } catch (error) {
        console.error('Logout error:', error);
    } finally {
        clearToken();
        window.location.href = '/login.html';
    }
}

async function getCurrentUser() {
    const token = getToken();
    if (!token) {
        throw new Error('未登录');
    }

    const response = await fetch(`${API_BASE}/users/me`, {
        headers: {
            'Authorization': `Bearer ${token}`
        }
    });

    if (!response.ok) {
        if (response.status === 401) {
            clearToken();
            window.location.href = '/login.html';
        }
        throw new Error('获取用户信息失败');
    }

    return await response.json();
}

async function updateUser(userId, data) {
    const token = getToken();
    const response = await fetch(`${API_BASE}/users/${userId}`, {
        method: 'PATCH',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    });

    if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || '更新失败');
    }

    return await response.json();
}

async function getAllUsers() {
    const token = getToken();
    const response = await fetch(`${API_BASE}/admin/users`, {
        headers: {
            'Authorization': `Bearer ${token}`
        }
    });

    if (!response.ok) {
        throw new Error('获取用户列表失败');
    }

    return await response.json();
}

async function deleteUser(userId) {
    const token = getToken();
    const response = await fetch(`${API_BASE}/users/${userId}`, {
        method: 'DELETE',
        headers: {
            'Authorization': `Bearer ${token}`
        }
    });

    if (!response.ok) {
        throw new Error('删除用户失败');
    }
}

function checkAuth() {
    const token = getToken();
    if (!token) {
        window.location.href = '/login.html';
    }
}

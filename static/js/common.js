const common = {
    async init_nav_links(user) {
        const link_div = document.querySelector('.nav-links');
        if (!user) {
            try {
                user = await getCurrentUser();
            } catch (error) {
                user = null;
            }
        }
        var html = `
            <a href="/html/index.html">首页</a>
            <a href="/html/watching.html" class="${location.pathname === '/html/watching.html' ? 'active' : '' }">盯盘</a>`;
        if (user) {
            html += `
            <a href="/html/stocks.html" class="${location.pathname === '/html/stocks.html' ? 'active' : '' }">持仓管理</a>
            <a href="/html/profile.html" class="${location.pathname === '/html/profile.html' ? 'active' : '' }">个人信息</a>`;
        }
        if (user?.is_superuser) {
            html += `
            <a href="/html/admin.html" class="${location.pathname === '/html/admin.html' ? 'active' : '' }">用户管理</a>
            <a href="/html/settings.html" class="${location.pathname === '/html/settings.html' ? 'active' : '' }">系统设置</a>`;
        }
        html += `
            <a href="/docs">文档</a>`;
        if (user) {
            html += `
            <a href="#" onclick="logout()">退出登录</a>`;
        } else {
            html += `
            <a href="/html/login.html">登录</a>
            <a href="/html/register.html">注册</a>`;
        }
        link_div.innerHTML = html;
    }
}

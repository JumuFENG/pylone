const common = {
    log(...args) {
        console.log(...args);
    },
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
    },
    rg_classname(v) {
        if (v > 0) {
            return 'red';
        } else if (v < 0) {
            return 'green';
        }
        return '';
    },
    removeAllChild(ele) {
        while(ele.hasChildNodes()) {
            ele.removeChild(ele.lastChild);
        }
    },
    stockAnchor(code, text=undefined) {
        var anchor = document.createElement('a');
        if (code.length > 6) {
            code = code.substring(2);
        }
        anchor.textContent = text;
        anchor.href = this.stockEmLink(code);
        anchor.target = '_blank';
        return anchor;
    },
    stockEmLink(code) {
        const emStockUrl = 'http://quote.eastmoney.com/concept/';
        const emStockUrlTail = '.html#fullScreenChart';
        return emStockUrl + (code.startsWith('60') || code.startsWith('68') ? 'sh' : 'sz') + code + emStockUrlTail;
    }
}

Number.prototype.toNarrowFixed = function(decimalPlaces) {
    const numStr = this.toFixed(decimalPlaces);
    if (!numStr.includes('.')) {
        return numStr;
    }

    let last0 = numStr.length - 1;
    while (last0 >= 0) {
        if (numStr[last0] !== '0') {
            break
        }
        last0 -= 1;
    }
    if (numStr[last0] === '.') {
        last0 -= 1;
    }
    return numStr.slice(0, last0 + 1);
};

class RadioAnchorPage {
    constructor(text) {
        this.createContainer();
        this.anchorBar = this.createAnchor(text);
        this.selected = false;
    }

    createContainer() {
        this.container = document.createElement('div');
        this.container.style.display = 'none';
    }

    createAnchor(text) {
        var anchor = document.createElement('a');
        anchor.href = '#';
        anchor.textContent = text;
        anchor.onclick = () => {
            if (this.onAnchorClicked) {
                this.onAnchorClicked(this.idx);
            }
        }
        return anchor;
    }

    show() {
        this.selected = true;
        this.anchorBar.className = 'highlight';
        this.container.style.display = 'block';
    }

    hide() {
        this.selected = false;
        this.anchorBar.className = '';
        this.container.style.display = 'none';
    }
}

class RadioAnchorBar {
    constructor(text = '') {
        this.container = document.createElement('div');
        this.container.className = 'radio_anchor_div';
        if (text.length > 0) {
            this.container.appendChild(document.createTextNode(text));
        };
        this.radioAchors = [];
    }

    clearAllAnchors() {
        if (this.radioAchors.length > 0) {
            this.radioAchors.forEach(a => {
                common.removeAllChild(a.container);
            });
            common.removeAllChild(this.container);
            this.radioAchors = [];
        }
    }

    addRadio(anpg) {
        this.container.appendChild(anpg.anchorBar);
        anpg.idx = this.radioAchors.length;
        anpg.onAnchorClicked = obj => {
            this.setHightlight(obj);
        }
        this.radioAchors.push(anpg);
    }

    setHightlight(i) {
        var h = this.getHighlighted();
        if (h == i) {
            return;
        }
        this.radioAchors[h].hide();
        this.radioAchors[i].show();
    }

    selectDefault() {
        var defaultItem = this.radioAchors[this.getHighlighted()];
        if (!defaultItem.selected) {
            defaultItem.show();
        }
    }

    getHighlighted() {
        for (var i = 0; i < this.radioAchors.length; i++) {
            if (this.radioAchors[i].selected) {
                return i;
            }
        };
        return 0;
    }
}

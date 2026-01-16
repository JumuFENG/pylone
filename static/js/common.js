const common = {
    log(...args) {
        console.log(...args);
    },
    public_links() {
        return `<a href="/html/watching.html" class="${location.pathname === '/html/watching.html' ? 'active' : '' }">盯盘</a>`;
    },
    normal_user_links() {
        return `
            <a href="/html/stocks.html" class="${location.pathname === '/html/stocks.html' ? 'active' : '' }">持仓管理</a>
            <a href="/html/deals.html" class="${location.pathname === '/html/deals.html' ? 'active' : '' }">成交管理</a>
            <a href="/html/profile.html" class="${location.pathname === '/html/profile.html' ? 'active' : '' }">个人信息</a>`;
    },
    admin_links() {
        return `
            <a href="/html/admin.html" class="${location.pathname === '/html/admin.html' ? 'active' : '' }">用户管理</a>
            <a href="/html/settings.html" class="${location.pathname === '/html/settings.html' ? 'active' : '' }">系统设置</a>`;
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
            <a href="/html/index.html">首页</a>`;
        html += this.public_links();
        if (user) {
            html += this.normal_user_links();
        }
        if (user?.is_superuser) {
            html += this.admin_links();
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
    },
    prc_calc: {
        init(price, zdf=10) {
            this.dirty = this.price != price;
            this.price = price;
            this.zdf = zdf;
        },
        targetTo(target) {
            if (!this.root || this.dirty) {
                if (this.root?.parentElement) {
                    this.root.parentElement.removeChild(this.root);
                }
                this.root = document.createElement('div');
                this.root.className = 'prc_calc_box';
                this.root.innerHTML = `
                    <div style="display: flex;flex-wrap: wrap;justify-content: space-between;">
                        <span style="font-weight: bold;font-size: 1.1em">简易计算器</span>
                        <label style="width: 100%">参考价: ${this.price} 
                            <input id="prc_input" style="width: 55px;" value="${this.price}">
                            <input id="pct_input" style="width: 35px;"><span>% = </span><span id="prc_calced"></span></label>
                        <label>涨停价: ${guang.calcZtPrice(this.price, this.zdf)}</label>
                        <label>+5%: ${guang.calcZtPrice(this.price, 5)}</label><label>+8%: ${guang.calcZtPrice(this.price, 8)} </label>
                        <label>跌停价: ${guang.calcDtPrice(this.price, this.zdf)}</label>
                        <label>-5%: ${guang.calcDtPrice(this.price, 5)}</label><label>-8%: ${guang.calcDtPrice(this.price, 8)}</label>
                    </div>`;
                this.root.querySelector('#pct_input').oninput = () => {
                    this.onInputChanged();
                }
                this.root.querySelector('#prc_input').oninput = () => {
                    this.onInputChanged();
                }
            }
            target.appendChild(this.root);
        },
        onInputChanged() {
            let prc = parseFloat(this.root.querySelector('#prc_input').value);
            let pct = parseFloat(this.root.querySelector('#pct_input').value);
            if (isNaN(prc) || isNaN(pct)) {
                return;
            }
            this.root.querySelector('#prc_calced').textContent = guang.calcZtPrice(prc, pct);
        }
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

class PrcCalculator {
    constructor(price, zdf=10) {
        this.price = price;
        this.zdf = zdf;
    }

    reset(price, zdf) {
        this.price = price;
        this.zdf = zdf;
    }
}

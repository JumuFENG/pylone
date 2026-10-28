class StrategyManager {
    constructor() {
        this.strategiesData = {};
        this.init();
    }

    init() {
        this.loadStrategies();
    }

    notify(message, type = 'info') {
        let box = document.getElementById('strategyToast');
        if (!box) {
            box = document.createElement('div');
            box.id = 'strategyToast';
            box.className = 'strategy-toast';
            document.body.appendChild(box);
        }
        box.textContent = message;
        box.className = `strategy-toast show ${type}`;
        clearTimeout(this._toastTimer);
        this._toastTimer = setTimeout(() => {
            box.className = 'strategy-toast';
        }, 2500);
    }

    async apiRequest(url, options = {}) {
        const defaultOptions = {
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json',
                ...(options.headers || {})
            }
        };
        const resp = await fetch(url, { ...defaultOptions, ...options });
        if (!resp.ok) {
            throw new Error(`API error: ${resp.status}`);
        }
        const ct = resp.headers.get('content-type') || '';
        if (ct.includes('application/json')) {
            return resp.json();
        }
        return resp.text();
    }

    async loadStrategies() {
        try {
            const data = await this.apiRequest('/user/iunstrs');
            this.strategiesData = data || {};
            this.updateStrategiesGrid(this.strategiesData);
        } catch (e) {
            console.error(e);
            this.notify('加载策略数据失败', 'error');
        }
    }

    updateStrategiesGrid(strategies) {
        const grid = document.getElementById('strategiesGrid');
        if (!grid) return;
        grid.innerHTML = '';
        Object.entries(strategies).forEach(([key, strategy]) => {
            const card = this.createStrategyCard(key, strategy);
            grid.appendChild(card);
        });
        const addCard = this.createAddStrategyCard();
        grid.appendChild(addCard);
    }

    createStrategyCard(key, strategy, isEditing = false) {
        const card = document.createElement('div');
        card.className = 'strategy-card';
        card.dataset.strategyKey = key;
        if (isEditing) {
            card.innerHTML = this.createStrategyEditForm(key, strategy);
        } else {
            card.innerHTML = this.createStrategyDisplayContent(key, strategy);
        }
        return card;
    }

    getCustomFields(strategy) {
        const reserved = ['key', 'enabled', 'amount', 'account', 'amtkey'];
        const fields = [];
        Object.keys(strategy || {}).forEach(k => {
            if (!reserved.includes(k)) {
                fields.push({ key: k, value: strategy[k] });
            }
        });
        return fields;
    }

    createStrategyDisplayContent(key, strategy) {
        const s = strategy || {};
        const customFields = this.getCustomFields(s);
        return `
            <div class="strategy-header">
                <div class="strategy-name">${s.key || key}</div>
                <div class="strategy-actions">
                    <div class="strategy-status ${s.enabled ? 'enabled' : 'disabled'}">
                        ${s.enabled ? '启用' : '禁用'}
                    </div>
                    <button class="btn btn-sm btn-icon-flat" onclick="strategyManager.editStrategy('${key}')">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-icon-flat btn-icon-danger" onclick="strategyManager.deleteStrategy('${key}')">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
            <div class="strategy-details">
                <div><strong>金额:</strong> ${s.amount || '0'}</div>
                <div><strong>账户:</strong> ${s.account || ''}</div>
                <div><strong>仓位方案:</strong> ${s.amtkey || '无'}</div>
                ${customFields.length > 0 ? `
                    <div class="custom-fields">
                        <strong>自定义参数:</strong>
                        ${customFields.map(f => `<div class="custom-field">${f.key}: ${f.value}</div>`).join('')}
                    </div>
                ` : ''}
            </div>
        `;
    }

    isCustomAccount(account) {
        if (!account) return false;
        return !['normal', 'credit', 'collateral', 'track'].includes(account);
    }

    createStrategyEditForm(key, strategy) {
        const s = strategy || {};
        const customFields = this.getCustomFields(s);
        return `
            <div class="strategy-header">
                <div class="strategy-name">${s.key || key}</div>
                <div class="strategy-actions">
                    <button class="btn btn-sm btn-success" onclick="strategyManager.saveStrategy('${key}')">
                        <i class="fas fa-save"></i> 保存
                    </button>
                    <button class="btn btn-sm btn-secondary" onclick="strategyManager.cancelEditStrategy('${key}')">
                        <i class="fas fa-times"></i> 取消
                    </button>
                </div>
            </div>
            <div class="strategy-edit-form">
                <div class="strategy-row">
                    <label>启用状态:</label>
                    <div class="checkbox-container">
                        <input type="checkbox" id="enabled_${key}" ${s.enabled ? 'checked' : ''}>
                        <label for="enabled_${key}" class="checkbox-label">启用策略</label>
                    </div>
                </div>
                <div class="strategy-row">
                    <label>金额:</label>
                    <input type="number" class="form-control inline-input" id="amount_${key}" value="${s.amount || ''}">
                </div>
                <div class="strategy-row">
                    <label>账户:</label>
                    <div class="account-input-container">
                        <select class="form-control account-select" id="accountSelect_${key}" onchange="strategyManager.handleAccountChange('${key}')">
                            <option value="">自动选择</option>
                            <option value="normal" ${s.account === 'normal' ? 'selected' : ''}>普通账户</option>
                            <option value="credit" ${s.account === 'credit' ? 'selected' : ''}>信用账户</option>
                            <option value="collateral" ${s.account === 'collateral' ? 'selected' : ''}>担保品账户</option>
                            <option value="custom" ${this.isCustomAccount(s.account) ? 'selected' : ''}>手动输入...</option>
                        </select>
                        <input type="text" class="form-control custom-account-input" id="accountCustom_${key}" value="${this.isCustomAccount(s.account) ? s.account : ''}" style="display:${this.isCustomAccount(s.account) ? 'block' : 'none'}" placeholder="输入账户名称">
                    </div>
                </div>
                <div class="strategy-row">
                    <label>仓位方案:</label>
                    <input type="text" class="form-control" id="amtkey_${key}" value="${s.amtkey || ''}">
                </div>
                <div class="custom-params-section">
                    <div class="custom-params-header">
                        <span>自定义参数</span>
                        <button type="button" class="btn btn-sm btn-outline-primary btn-add-custom" onclick="strategyManager.addCustomParam('${key}')"><i class="fas fa-plus"></i> 添加</button>
                    </div>
                    <div id="customParams_${key}">
                        ${customFields.map((f, idx) => `
                            <div class="custom-param-row">
                                <input type="text" class="custom-param-key" value="${f.key}" placeholder="参数名">
                                <input type="text" class="custom-param-value" value="${f.value}" placeholder="参数值">
                                <button type="button" class="btn-icon" onclick="strategyManager.removeCustomParam(this)"><i class="fas fa-trash"></i></button>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    handleAccountChange(key) {
        const sel = document.getElementById(`accountSelect_${key}`);
        const inp = document.getElementById(`accountCustom_${key}`);
        if (!sel || !inp) return;
        if (sel.value === 'custom') {
            inp.style.display = 'block';
        } else {
            inp.style.display = 'none';
        }
    }

    editStrategy(key) {
        const strategy = this.strategiesData[key];
        if (!strategy) return;
        const card = document.querySelector(`.strategy-card[data-strategy-key="${key}"]`);
        if (card) {
            card.innerHTML = this.createStrategyEditForm(key, strategy);
        }
    }

    cancelEditStrategy(key) {
        const strategy = this.strategiesData[key];
        if (!strategy) return;
        const card = document.querySelector(`.strategy-card[data-strategy-key="${key}"]`);
        if (card) {
            card.innerHTML = this.createStrategyDisplayContent(key, strategy);
        }
    }

    async saveStrategy(key) {
        const enabledEl = document.getElementById(`enabled_${key}`);
        const amountEl = document.getElementById(`amount_${key}`);
        const accountSelectEl = document.getElementById(`accountSelect_${key}`);
        const accountCustomEl = document.getElementById(`accountCustom_${key}`);
        const amtkeyEl = document.getElementById(`amtkey_${key}`);

        const enabled = enabledEl ? enabledEl.checked : false;
        const amount = amountEl ? amountEl.value : '';
        let account = accountSelectEl ? accountSelectEl.value : '';
        if (account === 'custom' && accountCustomEl) {
            account = accountCustomEl.value || '';
        } else if (account === '') {
            account = '';
        }
        const amtkey = amtkeyEl ? amtkeyEl.value : '';

        const strategy = {
            key: key,
            enabled: enabled,
            amount: amount,
            account: account,
            amtkey: amtkey
        };

        const rows = document.querySelectorAll(`#customParams_${key} .custom-param-row`);
        rows.forEach(row => {
            const k = row.querySelector('.custom-param-key').value.trim();
            const v = row.querySelector('.custom-param-value').value;
            if (k) {
                strategy[k] = v;
            }
        });

        this.strategiesData[key] = strategy;
        const card = document.querySelector(`.strategy-card[data-strategy-key="${key}"]`);
        if (card) {
            card.innerHTML = this.createStrategyDisplayContent(key, strategy);
        }
        await this.saveStrategyItem(key, strategy);
    }

    async saveStrategyItem(key, strategy) {
        try {
            await this.apiRequest(`/user/iunstrs/${encodeURIComponent(key)}`, {
                method: 'PUT',
                body: JSON.stringify(strategy)
            });
            this.notify('策略保存成功', 'success');
        } catch (e) {
            console.error(e);
            this.notify('策略保存失败', 'error');
        }
    }

    addCustomParam(key) {
        const container = document.getElementById(`customParams_${key}`);
        if (!container) return;
        const row = document.createElement('div');
        row.className = 'custom-param-row';
        row.innerHTML = `
            <input type="text" class="custom-param-key" placeholder="参数名">
            <input type="text" class="custom-param-value" placeholder="参数值">
            <button type="button" class="btn-icon" onclick="strategyManager.removeCustomParam(this)"><i class="fas fa-trash"></i></button>
        `;
        container.appendChild(row);
    }

    removeCustomParam(btn) {
        const row = btn.parentElement;
        if (row) row.remove();
    }

    createAddStrategyCard() {
        const card = document.createElement('div');
        card.className = 'add-strategy-card';
        card.innerHTML = `
            <div class="add-strategy-icon"><i class="fas fa-plus"></i></div>
            <div class="add-strategy-text">添加新策略</div>
        `;
        card.onclick = () => this.showAddStrategyForm();
        return card;
    }

    showAddStrategyForm() {
        const grid = document.getElementById('strategiesGrid');
        if (!grid || document.getElementById('addStrategyForm')) return;

        const card = document.createElement('div');
        card.className = 'strategy-card';
        card.id = 'addStrategyForm';
        card.innerHTML = `
            <div class="strategy-header">
                <div class="strategy-name">新增策略</div>
                <div class="strategy-actions">
                    <button class="btn btn-sm btn-success" onclick="strategyManager.confirmAddStrategy()">
                        <i class="fas fa-save"></i> 添加
                    </button>
                    <button class="btn btn-sm btn-secondary" onclick="strategyManager.cancelAddStrategy()">
                        <i class="fas fa-times"></i> 取消
                    </button>
                </div>
            </div>
            <div class="strategy-row">
                <label>标识符:</label>
                <input type="text" class="form-control" id="newStrategyKey" placeholder="istrategy_xxx">
            </div>
            <div class="strategy-row">
                <label>金额:</label>
                <input type="number" class="form-control inline-input" id="newStrategyAmount" value="">
            </div>
            <div class="strategy-row">
                <label>账户:</label>
                <select class="form-control account-select" id="newStrategyAccount">
                    <option value="">自动选择</option>
                    <option value="normal">普通账户</option>
                    <option value="credit">信用账户</option>
                    <option value="collateral">担保品账户</option>
                </select>
            </div>
        `;
        const addCard = grid.querySelector('.add-strategy-card');
        if (addCard) {
            grid.insertBefore(card, addCard);
        } else {
            grid.appendChild(card);
        }
        const keyInput = document.getElementById('newStrategyKey');
        if (keyInput) keyInput.focus();
    }

    cancelAddStrategy() {
        const form = document.getElementById('addStrategyForm');
        if (form) form.remove();
    }

    async confirmAddStrategy() {
        const keyInput = document.getElementById('newStrategyKey');
        const amountInput = document.getElementById('newStrategyAmount');
        const accountInput = document.getElementById('newStrategyAccount');
        const key = keyInput ? keyInput.value.trim() : '';
        if (!key) {
            this.notify('请输入策略标识符', 'error');
            return;
        }
        if (this.strategiesData[key]) {
            this.notify('该策略标识符已存在', 'error');
            return;
        }
        const strategy = {
            key: key,
            enabled: false,
            amount: amountInput ? amountInput.value : '',
            account: accountInput ? accountInput.value : '',
            amtkey: ''
        };
        try {
            await this.apiRequest(`/user/iunstrs/${encodeURIComponent(key)}`, {
                method: 'POST',
                body: JSON.stringify(strategy)
            });
        } catch (e) {
            console.error(e);
            this.notify('新增策略失败', 'error');
            return;
        }
        this.notify('策略添加成功', 'success');
        this.cancelAddStrategy();
        this.strategiesData[key] = strategy;
        this.updateStrategiesGrid(this.strategiesData);
        setTimeout(() => {
            this.editStrategy(key);
        }, 100);
    }

    async deleteStrategy(key) {
        if (!window.confirm('确定要删除该策略吗？')) return;
        try {
            await this.apiRequest(`/user/iunstrs/${encodeURIComponent(key)}`, {
                method: 'DELETE'
            });
        } catch (e) {
            console.error(e);
            this.notify('删除策略失败', 'error');
            return;
        }
        delete this.strategiesData[key];
        this.updateStrategiesGrid(this.strategiesData);
        this.notify('策略已删除', 'success');
    }
}

const strategyManager = new StrategyManager();

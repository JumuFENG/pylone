window.xreq = function(m) {return window;}
guang.dserver = location.origin + '/';
guang.isweb = true;

let capitalChart = null;
let paramsExpanded = true;
let cachedSignalData = null;
let cachedRequestParams = null;

const CONDITION_FORMULAS = {
    buy: {
        'ma_cross': (params) => {
            const fast = params.fast_period || 5;
            const slow = params.slow_period || 10;
            return params.direction === 'up' 
                ? `CROSS(MA(C,${fast}),MA(C,${slow}))`
                : `CROSS(MA(C,${slow}),MA(C,${fast}))`;
        },
        'consecutive_up': (params) => `EVERY(C>REF(C,1),${params.period || 2})`,
        'consecutive_down': (params) => `EVERY(C<REF(C,1),${params.period || 2})`,
        'break_high': (params) => `C>HHV(H,${params.period || 20})`,
        'break_low': (params) => `C<LLV(L,${params.period || 20})`,
        'volume_spike': (params) => `V>HHV(V,${params.period || 20})*2`,
        'ma_angle': (params) => `EMA(C,${params.fast_period || 5})>REF(EMA(C,${params.fast_period || 5}),1)`
    },
    sell: {
        'ma_cross': (params) => {
            const fast = params.fast_period || 5;
            const slow = params.slow_period || 10;
            return `CROSS(MA(C,${slow}),MA(C,${fast}))`;
        },
        'break_low': (params) => `C<LLV(L,${params.period || 20})`,
        'consecutive_down': (params) => `EVERY(C<REF(C,1),${params.period || 3})`
    }
};

const PRESETS = {
    'ma多头': {
        buy_condition: { type: 'ma_cross', fast_period: 5, slow_period: 10, direction: 'up', price_type: 'close' },
        sell_conditions: [
            { type: 'ma_cross', fast_period: 5, slow_period: 20, direction: 'down', price_type: 'close' },
            { type: 'profit_target', pct: 20, price_type: 'trigger' },
            { type: 'stop_loss', pct: -5, price_type: 'trigger' }
        ]
    },
    'ma_cross': {
        buy_condition: { type: 'ma_cross', fast_period: 5, slow_period: 10, direction: 'up', price_type: 'close' },
        sell_conditions: [
            { type: 'ma_cross', fast_period: 5, slow_period: 10, direction: 'down', price_type: 'close' },
            { type: 'profit_target', pct: 10, price_type: 'trigger' },
            { type: 'stop_loss', pct: -5, price_type: 'trigger' }
        ]
    },
    'break_high': {
        buy_condition: { type: 'break_high', period: 20, price_type: 'trigger' },
        sell_conditions: [
            { type: 'profit_target', pct: 15, price_type: 'trigger' },
            { type: 'hold_days', days: 3, price_type: 'next_open' },
            { type: 'stop_loss', pct: -5, price_type: 'trigger' }
        ]
    },
    'consecutive_up': {
        buy_condition: { type: 'consecutive_up', period: 2, price_type: 'close' },
        sell_conditions: [
            { type: 'profit_target', pct: 10, price_type: 'trigger' },
            { type: 'hold_days', days: 2, price_type: 'next_open' },
            { type: 'stop_loss', pct: -3, price_type: 'trigger' }
        ]
    }
};

function toggleParams() {
    paramsExpanded = !paramsExpanded;
    const body = document.getElementById('params-body');
    const icon = document.getElementById('toggle-icon');
    body.style.display = paramsExpanded ? 'block' : 'none';
    icon.textContent = paramsExpanded ? '▼' : '▶';
}

function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
    document.getElementById(tabId).classList.add('active');
}

function initDates() {
    const today = new Date();
    const oneYearAgo = new Date(today);
    oneYearAgo.setFullYear(today.getFullYear() - 1);
    document.getElementById('end_date').value = formatDate(today);
    document.getElementById('start_date').value = formatDate(oneYearAgo);
}

function formatDate(date) {
    return date.toISOString().split('T')[0];
}

function loadPreset() {
    const preset = document.getElementById('preset_select').value;
    if (!preset || preset === 'custom') return;
    
    const config = PRESETS[preset];
    if (!config) return;
    
    document.getElementById('buy_condition_type').value = config.buy_condition.type;
    updateBuyConditionUI();
    
    if (config.buy_condition.fast_period) document.getElementById('fast_period').value = config.buy_condition.fast_period;
    if (config.buy_condition.slow_period) document.getElementById('slow_period').value = config.buy_condition.slow_period;
    if (config.buy_condition.direction) document.getElementById('direction').value = config.buy_condition.direction;
    if (config.buy_condition.period) document.getElementById('period').value = config.buy_condition.period;
    if (config.buy_condition.price_type) document.getElementById('buy_price_type').value = config.buy_condition.price_type;
    
    const sellList = document.getElementById('sell_conditions_list');
    sellList.innerHTML = '';
    
    // Set sell add form defaults from first sell condition (for formula types)
    const firstSellCond = config.sell_conditions[0];
    if (firstSellCond) {
        if (firstSellCond.fast_period) document.getElementById('sell_add_fast').value = firstSellCond.fast_period;
        if (firstSellCond.slow_period) document.getElementById('sell_add_slow').value = firstSellCond.slow_period;
        if (firstSellCond.period) document.getElementById('sell_add_period').value = firstSellCond.period;
    }
    
    config.sell_conditions.forEach(cond => {
        let value = '';
        if (cond.pct !== undefined) value = cond.pct;
        else if (cond.days !== undefined) value = cond.days;
        else if (cond.slow_period !== undefined) {
            value = cond.slow_period;
            document.getElementById('sell_add_slow').value = cond.slow_period;
        }
        else if (cond.period !== undefined) {
            value = cond.period;
            document.getElementById('sell_add_period').value = cond.period;
        }
        else if (cond.fast_period !== undefined) value = cond.fast_period;
        addSellCondition(cond, value);
    });
}

function updateBuyConditionUI() {
    const type = document.getElementById('buy_condition_type').value;
    const paramsDiv = document.getElementById('buy_params');
    const periodGroup = document.getElementById('period_group');
    const formulaInput = document.getElementById('buy_formula_input');
    
    if (type === 'ma_cross') {
        paramsDiv.style.display = '';
        periodGroup.style.display = 'none';
        formulaInput.classList.remove('expanded');
        generateBuyFormula();
    } else if (type === 'volume_spike') {
        paramsDiv.style.display = 'none';
        periodGroup.style.display = '';
        formulaInput.classList.remove('expanded');
        generateBuyFormula();
    } else if (type === 'formula') {
        paramsDiv.style.display = 'none';
        periodGroup.style.display = 'none';
        formulaInput.classList.add('expanded');
        formulaInput.placeholder = '请输入自定义公式';
        formulaInput.readOnly = false;
    } else {
        paramsDiv.style.display = 'none';
        periodGroup.style.display = '';
        formulaInput.classList.remove('expanded');
        generateBuyFormula();
    }
}

function generateBuyFormula() {
    const type = document.getElementById('buy_condition_type').value;
    const formulaInput = document.getElementById('buy_formula_input');
    
    if (type === 'formula') return;
    
    const params = {
        fast_period: parseInt(document.getElementById('fast_period')?.value) || 5,
        slow_period: parseInt(document.getElementById('slow_period')?.value) || 10,
        period: parseInt(document.getElementById('period')?.value) || 20,
        direction: document.getElementById('direction')?.value || 'up'
    };
    
    if (CONDITION_FORMULAS.buy[type]) {
        formulaInput.value = CONDITION_FORMULAS.buy[type](params);
        formulaInput.readOnly = true;
    }
}

function updateSellAddForm() {
    const type = document.getElementById('sell_add_type').value;
    const pctGroup = document.querySelector('#sell_add_params .sell-pct-group');
    const maGroup = document.querySelector('#sell_add_params .sell-ma-group');
    const periodGroup = document.querySelector('#sell_add_params .sell-period-group');
    const daysGroup = document.querySelector('#sell_add_params .sell-days-group');
    const formulaGroup = document.querySelector('#sell_add_params .sell-formula-group');
    
    pctGroup.style.display = 'none';
    maGroup.style.display = 'none';
    periodGroup.style.display = 'none';
    daysGroup.style.display = 'none';
    formulaGroup.style.display = 'none';
    
    if (type === 'profit_target' || type === 'stop_loss') {
        pctGroup.style.display = '';
    } else if (type === 'ma_cross') {
        maGroup.style.display = '';
    } else if (type === 'break_low' || type === 'consecutive_down') {
        periodGroup.style.display = '';
    } else if (type === 'hold_days') {
        daysGroup.style.display = '';
    } else if (type === 'formula') {
        formulaGroup.style.display = '';
    }
}

function addSellCondition(cond, value) {
    // Use provided values or fall back to DOM
    const condType = cond?.type || document.getElementById('sell_add_type').value;
    const condPriceType = cond?.price_type || document.getElementById('sell_add_price_type').value;
    const list = document.getElementById('sell_conditions_list');
    
    let name = '', formulaValue = '', formula = '';
    
    if (condType === 'profit_target') {
        const pct = value !== undefined ? parseFloat(value) : (parseFloat(document.getElementById('sell_add_pct').value) || 20);
        name = '止盈';
        formulaValue = pct + '%';
    } else if (condType === 'stop_loss') {
        let pct = value !== undefined ? parseFloat(value) : (parseFloat(document.getElementById('sell_add_pct').value) || 20);
        if (pct > 0) pct = -pct;
        name = '止损';
        formulaValue = pct + '%';
    } else if (condType === 'hold_days') {
        const days = value !== undefined ? parseInt(value) : (parseInt(document.getElementById('sell_add_days').value) || 3);
        name = '持仓';
        formulaValue = days + '天';
    } else if (condType === 'ma_cross') {
        const fast = cond?.fast_period || document.getElementById('sell_add_fast').value || 5;
        const slow = cond?.slow_period || document.getElementById('sell_add_slow').value || 10;
        name = 'MA死叉';
        formula = `CROSS(MA(C,${slow}),MA(C,${fast}))`;
    } else if (condType === 'break_low') {
        const period = value || document.getElementById('sell_add_period').value || 20;
        name = '跌破低点';
        formula = `C<LLV(L,${period})`;
    } else if (condType === 'consecutive_down') {
        const period = value || document.getElementById('sell_add_period').value || 3;
        name = '连跌';
        formula = `EVERY(C<REF(C,1),${period})`;
    } else if (condType === 'formula') {
        name = '公式';
        formula = document.getElementById('sell_add_formula').value || '';
    }
    
    const item = document.createElement('div');
    item.className = 'sell-condition-item';
    item.setAttribute('data-type', condType);
    item.setAttribute('data-price-type', condPriceType);
    item.setAttribute('data-formula', formula);
    item.setAttribute('data-value', formulaValue);
    
    const priceTypeText = condPriceType === 'trigger' ? '触发价' : condPriceType === 'next_open' ? '次开' : condPriceType === 'close' ? '收盘' : condPriceType === 'low' ? '最低' : '最高';
    const priceOptions = `trigger=触发价,next_open=次开,close=收盘,low=最低,high=最高`.split(',').map(o => {
        const [v, t] = o.split('=');
        return `<option value="${v}" ${v === condPriceType ? 'selected' : ''}>${t}</option>`;
    }).join('');
    
    if (condType === 'formula') {
        item.innerHTML = `
            <button type="button" class="btn-remove" title="删除" onclick="this.parentElement.remove()">×</button>
            <span class="sell-condition-name" title="${name}">${name}</span>
            <select class="sell-price-type" title="卖出价格" onchange="this.closest('.sell-condition-item').setAttribute('data-price-type', this.value)">
                ${priceOptions}
            </select>
            <input type="text" class="sell-condition-value" value="${formula}" onchange="this.closest('.sell-condition-item').setAttribute('data-formula', this.value)">
        `;
    } else if (condType === 'profit_target' || condType === 'stop_loss' || condType === 'hold_days') {
        item.innerHTML = `
            <button type="button" class="btn-remove" title="删除" onclick="this.parentElement.remove()">×</button>
            <span class="sell-condition-name" title="${name}">${name}</span>
            <select class="sell-price-type" title="卖出价格" onchange="this.closest('.sell-condition-item').setAttribute('data-price-type', this.value)">
                ${priceOptions}
            </select>
            <input type="text" class="sell-condition-value" value="${formulaValue}" onchange="this.closest('.sell-condition-item').setAttribute('data-value', this.value)">
        `;
    } else {
        item.innerHTML = `
            <button type="button" class="btn-remove" title="删除" onclick="this.parentElement.remove()">×</button>
            <span class="sell-condition-name" title="${name}">${name}</span>
            <select class="sell-price-type" title="卖出价格" onchange="this.closest('.sell-condition-item').setAttribute('data-price-type', this.value)">
                ${priceOptions}
            </select>
            <input type="text" class="sell-condition-value" value="${formula}" readonly>
        `;
    }
    
    list.appendChild(item);
}

async function loadUserInfo() {
    try {
        const currentUser = await getCurrentUser();
        common.init_nav_links(currentUser);
        document.getElementById('loading').style.display = 'none';
    } catch (error) {
        showError(error.message);
        document.getElementById('loading').style.display = 'none';
    }
}

async function getSignals() {
    const buyType = document.getElementById('buy_condition_type').value;
    const buyPriceType = document.getElementById('buy_price_type').value;
    
    const buyParams = {
        type: buyType,
        fast_period: parseInt(document.getElementById('fast_period').value) || 5,
        slow_period: parseInt(document.getElementById('slow_period').value) || 10,
        direction: document.getElementById('direction').value,
        period: parseInt(document.getElementById('period').value) || 20,
        price_type: buyPriceType
    };
    
    const buyFormulaInput = document.getElementById('buy_formula_input')?.value?.trim();
    let buyFormula = '';
    if (buyType === 'formula') {
        buyFormula = buyFormulaInput || '';
    } else if (buyFormulaInput) {
        buyFormula = buyFormulaInput;
    } else if (CONDITION_FORMULAS.buy[buyType]) {
        buyFormula = CONDITION_FORMULAS.buy[buyParams];
    }
    
    const sellConditions = [];
    
    document.querySelectorAll('.sell-condition-item').forEach(item => {
        const type = item.getAttribute('data-type');
        const priceType = item.getAttribute('data-price-type') || 'next_open';
        const formula = item.getAttribute('data-formula') || '';
        const value = item.getAttribute('data-value') || '';
        
        if (type === 'profit_target') {
            const pct = parseFloat(value.replace('%', '')) || 20;
            sellConditions.push({ type: 'profit_target', pct: pct, price_type: priceType, formula: '' });
        } else if (type === 'stop_loss') {
            const pct = parseFloat(value.replace('%', '')) || -5;
            sellConditions.push({ type: 'stop_loss', pct: pct, price_type: priceType, formula: '' });
        } else if (type === 'hold_days') {
            const days = parseInt(value.replace('天', '')) || 3;
            sellConditions.push({ type: 'hold_days', days: days, price_type: priceType, formula: '' });
        } else {
            sellConditions.push({ type, formula, price_type: priceType });
        }
    });
    
    const request = {
        start_date: document.getElementById('start_date').value,
        end_date: document.getElementById('end_date').value,
        stock_pool: document.getElementById('stock_pool').value,
        buy_condition: { ...buyParams, formula: buyFormula },
        sell_conditions: sellConditions
    };
    
    cachedRequestParams = {
        ...request,
        buy_price_type: buyPriceType,
        init_capital: parseFloat(document.getElementById('init_capital').value) || 100000
    };
    
    const btn = document.getElementById('get-signals');
    btn.disabled = true;
    btn.textContent = '获取信号中...';
    
    try {
        const response = await fetch(`${API_BASE}/api/backtest/signals`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify(request)
        });
        
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || '获取信号失败');
        }
        
        const signalData = await response.json();
        
        cachedSignalData = signalData;
        cachedRequestParams = {
            ...request,
            buy_price_type: buyPriceType,
            init_capital: parseFloat(document.getElementById('init_capital').value) || 100000
        };
        
        btn.textContent = '模拟交易...';
        
        const result = simulateTrade(signalData, cachedRequestParams);
        displayResults(result);
        
    } catch (error) {
        showError(error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = '获取买卖信号';
    }
}

function simulateTrade(signalData, params) {
    const initCapital = params.init_capital || 100000;
    const feeRate = parseFloat(document.getElementById('fee_rate').value) || 0.0003;
    const maxPositions = parseInt(document.getElementById('max_positions').value);
    const dailyLimit = parseInt(document.getElementById('daily_limit').value);
    const buyMode = document.getElementById('buy_mode').value;
    const buyAmount = parseFloat(document.getElementById('buy_amount').value);
    const buyPriceType = params.buy_price_type || 'next_open';
    
    let cash = initCapital;
    const positions = {};
    const trades = [];
    const dailyStats = [];
    
    const dates = signalData.dates || [];
    const signals = signalData.signals || {};
    
    for (const date of dates) {
        const dailySignals = signals[date] || { buy: [], sell: [] };
        
        // Process sell signals from backend (includes formula-based and dynamic conditions)
        for (const sig of dailySignals.sell || []) {
            if (positions[sig.code]) {
                const pos = positions[sig.code];
                const sellPrice = sig.price || pos.current_price || pos.buy_price;
                const amount = sellPrice * pos.quantity;
                const fee = amount * feeRate;
                const profit = amount - (pos.buy_price * pos.quantity) - fee;
                const profitPct = (profit / (pos.buy_price * pos.quantity)) * 100;
                
                cash += (amount - fee);
                trades.push({
                    date: date, code: sig.code, name: sig.name, action: 'sell',
                    price: sellPrice, quantity: pos.quantity, amount: amount, fee: fee,
                    reason: sig.reason, profit: profit, profit_pct: profitPct
                });
                delete positions[sig.code];
            }
        }
        
        // Process buy signals
        if (dailySignals.buy && dailySignals.buy.length > 0 && Object.keys(positions).length < maxPositions) {
            const available = maxPositions - Object.keys(positions).length;
            const toBuy = dailySignals.buy.slice(0, Math.min(available, dailyLimit));
            
            for (const sig of toBuy) {
                const price = sig.price || 10;
                const amount = buyMode === 'fixed' ? buyAmount : cash * (buyAmount / 100);
                if (amount < 1000) continue;
                
                const quantity = Math.floor(amount / price / 100) * 100;
                if (quantity < 100) continue;
                
                const totalAmount = price * quantity;
                const fee = totalAmount * feeRate;
                if (totalAmount + fee > cash) continue;
                
                cash -= (totalAmount + fee);
                positions[sig.code] = {
                    code: sig.code, name: sig.name, buy_date: date, buy_price: price,
                    current_price: price, quantity: quantity, buy_reason: sig.reason
                };
                
                trades.push({
                    date: date, code: sig.code, name: sig.name, action: 'buy',
                    price: price, quantity: quantity, amount: totalAmount, fee: fee,
                    reason: sig.reason, profit: null, profit_pct: null
                });
            }
        }
        
        let capital = cash;
        for (const code in positions) {
            capital += positions[code].current_price * positions[code].quantity;
        }
        
        dailyStats.push({
            date: date, capital: capital,
            positions: Object.keys(positions).length,
            trades: trades.filter(t => t.date === date && t.action === 'sell').length,
            daily_profit: capital - initCapital
        });
    }
    
    const sellTrades = trades.filter(t => t.action === 'sell');
    const totalTrades = sellTrades.length;
    const winningTrades = sellTrades.filter(t => t.profit > 0).length;
    const losingTrades = sellTrades.filter(t => t.profit <= 0).length;
    const winRate = totalTrades > 0 ? (winningTrades / totalTrades * 100) : 0;
    const totalProfit = sellTrades.reduce((sum, t) => sum + (t.profit || 0), 0);
    const totalProfitPct = (totalProfit / initCapital) * 100;
    const avgProfit = totalTrades > 0 ? totalProfit / totalTrades : 0;
    
    let maxDrawdown = 0, peak = initCapital;
    for (const stat of dailyStats) {
        if (stat.capital > peak) peak = stat.capital;
        const dd = ((peak - stat.capital) / peak) * 100;
        if (dd > maxDrawdown) maxDrawdown = dd;
    }
    
    return {
        total_trades: totalTrades, winning_trades: winningTrades, losing_trades: losingTrades,
        win_rate: winRate, total_profit: totalProfit, total_profit_pct: totalProfitPct,
        max_drawdown: maxDrawdown, avg_profit: avgProfit,
        daily_stats: dailyStats, trades: trades
    };
}

function displayResults(result) {
    document.getElementById('empty-state').style.display = 'none';
    document.getElementById('results-section').style.display = 'block';
    
    document.getElementById('total-profit-pct').textContent = result.total_profit_pct.toFixed(2) + '%';
    document.getElementById('total-profit-pct').style.color = result.total_profit >= 0 ? '#ff4d4f' : '#52c41a';
    
    document.getElementById('win-rate').textContent = result.win_rate.toFixed(2) + '%';
    document.getElementById('total-trades').textContent = result.total_trades;
    document.getElementById('max-drawdown').textContent = result.max_drawdown.toFixed(2) + '%';
    document.getElementById('winning-trades').textContent = result.winning_trades;
    document.getElementById('losing-trades').textContent = result.losing_trades;
    
    renderCapitalChart(result.daily_stats);
    renderTradeTable(result.trades);
}

function runBacktest() {
    if (!cachedSignalData) {
        showError('请先获取买卖信号');
        return;
    }
    
    const btn = document.getElementById('run-backtest');
    btn.disabled = true;
    btn.textContent = '回测中...';
    
    try {
        const result = simulateTrade(cachedSignalData, cachedRequestParams);
        displayResults(result);
    } catch (error) {
        showError(error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = '再次回测';
    }
}

function renderCapitalChart(dailyStats) {
    const chartDom = document.getElementById('capital-chart');
    if (capitalChart) capitalChart.dispose();
    capitalChart = echarts.init(chartDom);
    
    const option = {
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: dailyStats.map(s => s.date), axisLabel: { rotate: 45, fontSize: 10 } },
        yAxis: { type: 'value', scale: true },
        series: [{
            data: dailyStats.map(s => s.capital), type: 'line', smooth: true,
            lineStyle: { color: '#1890ff' },
            areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: 'rgba(24, 144, 255, 0.3)' },
                { offset: 1, color: 'rgba(24, 144, 255, 0.05)' }
            ])}
        }]
    };
    capitalChart.setOption(option);
}

function renderTradeTable(trades) {
    const tbody = document.getElementById('trade-tbody');
    tbody.innerHTML = '';
    
    trades.forEach(trade => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${trade.date}</td>
            <td>${trade.name} (${trade.code})</td>
            <td style="color: ${trade.action === 'buy' ? '#ff4d4f' : '#52c41a'}">${trade.action === 'buy' ? '买入' : '卖出'}</td>
            <td>${trade.price.toFixed(2)}</td>
            <td>${trade.quantity}</td>
            <td>${trade.amount.toFixed(2)}</td>
            <td>${trade.fee.toFixed(2)}</td>
            <td>${trade.reason}</td>
            <td style="color: ${trade.profit >= 0 ? '#ff4d4f' : '#52c41a'}">${trade.profit ? trade.profit.toFixed(2) : '-'}</td>
            <td style="color: ${trade.profit_pct >= 0 ? '#ff4d4f' : '#52c41a'}">${trade.profit_pct ? trade.profit_pct.toFixed(2) + '%' : '-'}</td>
        `;
        tbody.appendChild(row);
    });
}

function showError(msg) {
    const alertDiv = document.getElementById('alert');
    alertDiv.textContent = msg;
    alertDiv.className = 'alert alert-error';
    alertDiv.style.display = 'block';
    setTimeout(() => { alertDiv.style.display = 'none'; }, 3000);
}

window.addEventListener('DOMContentLoaded', () => { 
    document.querySelectorAll('input, select, textarea').forEach(el => el.setAttribute('autocomplete', 'off'));
    initDates(); 
    loadUserInfo(); 
    generateBuyFormula();
    updateSellAddForm();
});

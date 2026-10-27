guang.dserver = location.origin + '/';
guang.isweb = true;

let klineChart = null;

function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
    document.getElementById(tabId).classList.add('active');
}

function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}

async function validateFormula() {
    const code = document.getElementById('stock_code').value.trim();
    const buyFormula = document.getElementById('buy_formula').value.trim();
    const sellFormula = document.getElementById('sell_formula').value.trim();
    const kltype = document.getElementById('kltype').value;
    const length = parseInt(document.getElementById('kline_length').value) || 250;
    
    if (!code) {
        showError('请输入股票代码');
        return;
    }
    if (!buyFormula) {
        showError('请输入买入公式');
        return;
    }
    
    const btn = document.getElementById('validate-btn');
    btn.disabled = true;
    btn.textContent = '验证中...';
    
    try {
        const params = new URLSearchParams({
            code: code,
            buy_formula: buyFormula,
            sell_formula: sellFormula,
            kltype: kltype,
            length: length
        });
        
        const response = await fetch(`${API_BASE}/api/backtest/validate?${params}`, {
            method: 'GET',
            credentials: 'include'
        });
        
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || '验证失败');
        }
        
        const result = await response.json();
        displayResults(result);
        
    } catch (error) {
        showError(error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = '验证公式';
    }
}

function displayResults(result) {
    document.getElementById('empty-state').style.display = 'none';
    document.getElementById('results-section').style.display = 'block';
    
    document.getElementById('buy-count').textContent = result.buy_count;
    document.getElementById('sell-count').textContent = result.sell_count;
    document.getElementById('kline-count').textContent = result.klines.length;
    
    renderKlineChart(result.klines, result.buy_signals, result.sell_signals);
    renderSignalsTable(result.buy_signals, result.sell_signals);
}

function calcMA(closeData, period) {
    const ma = [];
    for (let i = 0; i < closeData.length; i++) {
        if (i < period - 1) {
            ma.push('-');
        } else {
            let sum = 0;
            for (let j = 0; j < period; j++) {
                sum += closeData[i - j];
            }
            ma.push((sum / period).toFixed(2));
        }
    }
    return ma;
}

function calcEMA(closeData, period) {
    const ema = [];
    const k = 2 / (period + 1);
    for (let i = 0; i < closeData.length; i++) {
        if (i === 0) {
            ema.push(closeData[i].toFixed(2));
        } else if (i < period - 1) {
            let sum = 0;
            for (let j = 0; j <= i; j++) {
                sum += closeData[j];
            }
            ema.push((sum / (i + 1)).toFixed(2));
        } else {
            const prevEma = parseFloat(ema[i - 1]);
            const newEma = ((closeData[i] - prevEma) * k + prevEma).toFixed(2);
            ema.push(newEma);
        }
    }
    return ema;
}

function renderKlineChart(klines, buySignals, sellSignals) {
    const chartDom = document.getElementById('kline-chart');
    if (klineChart) klineChart.dispose();
    klineChart = echarts.init(chartDom);
    
    const dates = klines.map(k => k.time);
    const ohlc = klines.map(k => [k.open, k.close, k.low, k.high]);
    const volumes = klines.map(k => k.volume);
    const closeData = klines.map(k => parseFloat(k.close));
    
    const ma5 = calcMA(closeData, 5);
    const ma10 = calcMA(closeData, 10);
    const ma20 = calcMA(closeData, 20);
    const ma30 = calcMA(closeData, 30);
    const ema12 = calcEMA(closeData, 12);
    const ema26 = calcEMA(closeData, 26);
    
    const klineMap = {};
    klines.forEach((k, i) => {
        klineMap[k.time] = {
            high: parseFloat(k.high),
            low: parseFloat(k.low),
            open: parseFloat(k.open),
            close: parseFloat(k.close)
        };
    });
    
    const buyMarks = buySignals.map(sig => {
        const k = klineMap[sig.date];
        if (!k) return null;
        const price = sig.price;
        const offset = (k.high - k.low) * 0.15;
        return [sig.date, price + offset];
    }).filter(m => m !== null);
    
    const sellMarks = sellSignals.map(sig => {
        const k = klineMap[sig.date];
        if (!k) return null;
        const price = sig.price;
        const offset = (k.high - k.low) * 0.15;
        return [sig.date, price - offset];
    }).filter(m => m !== null);
    
    const option = {
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'cross' },
            formatter: function(params) {
                const kline = params.find(p => p.seriesName === 'K线');
                if (!kline || !kline.data) return '';
                const d = kline.data;
                const date = kline.axisValue;
                
                if (!Array.isArray(d) || d.length < 4) {
                    return `${date}<br/>数据格式错误`;
                }
                
                let html = `<strong>${date}</strong><br/>`;
                html += `开: ${parseFloat(d[0]).toFixed(2)} &nbsp; 收: ${parseFloat(d[1]).toFixed(2)}<br/>`;
                html += `高: ${parseFloat(d[2]).toFixed(2)} &nbsp; 低: ${parseFloat(d[3]).toFixed(2)}`;
                params.forEach(p => {
                    if (['MA5', 'MA10', 'MA20', 'MA30', 'EMA12', 'EMA26'].includes(p.seriesName)) {
                        if (p.value !== '-' && p.value !== undefined && !isNaN(p.value)) {
                            html += `<br/>${p.marker} ${p.seriesName}: ${parseFloat(p.value).toFixed(2)}`;
                        }
                    }
                });
                return html;
            }
        },
        legend: {
            data: ['K线', 'MA5', 'MA10', 'MA20', 'MA30', 'EMA12', 'EMA26', '买入', '卖出', '成交量'],
            top: 30
        },
        grid: [
            { left: '10%', right: '10%', top: 80, height: '45%' },
            { left: '10%', right: '10%', top: '75%', height: '15%' }
        ],
        xAxis: [
            { type: 'category', data: dates, boundaryGap: true, axisLine: { onZero: false }, splitLine: { show: false } },
            { type: 'category', gridIndex: 1, data: dates, boundaryGap: true, axisLine: { onZero: false }, axisLabel: { show: false }, splitLine: { show: false } }
        ],
        yAxis: [
            { scale: true, splitLine: { show: true } },
            { scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { show: false }, axisLine: { show: false }, axisTick: { show: false }, splitLine: { show: false } }
        ],
        dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1], start: 50, end: 100 },
            { show: true, xAxisIndex: [0, 1], type: 'slider', top: '92%', start: 50, end: 100 }
        ],
        series: [
            {
                name: 'K线',
                type: 'candlestick',
                data: ohlc,
                itemStyle: {
                    color: '#ff4d4f',
                    color0: '#52c41a',
                    borderColor: '#ff4d4f',
                    borderColor0: '#52c41a'
                }
            },
            { name: 'MA5', type: 'line', data: ma5, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
            { name: 'MA10', type: 'line', data: ma10, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
            { name: 'MA20', type: 'line', data: ma20, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
            { name: 'MA30', type: 'line', data: ma30, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
            { name: 'EMA12', type: 'line', data: ema12, smooth: true, lineStyle: { width: 1, type: 'dashed' }, symbol: 'none' },
            { name: 'EMA26', type: 'line', data: ema26, smooth: true, lineStyle: { width: 1, type: 'dashed' }, symbol: 'none' },
            {
                name: '买入',
                type: 'scatter',
                data: buyMarks,
                symbol: 'circle',
                symbolSize: 0,
                itemStyle: { color: 'transparent' },
                label: {
                    show: true,
                    position: 'top',
                    formatter: '▲',
                    color: '#ff4d4f',
                    fontSize: 12
                },
                z: 10
            },
            {
                name: '卖出',
                type: 'scatter',
                data: sellMarks,
                symbol: 'circle',
                symbolSize: 0,
                itemStyle: { color: 'transparent' },
                label: {
                    show: true,
                    position: 'bottom',
                    formatter: '▼',
                    color: '#52c41a',
                    fontSize: 12
                },
                z: 10
            },
            {
                name: '成交量',
                type: 'bar',
                xAxisIndex: 1,
                yAxisIndex: 1,
                data: volumes,
                itemStyle: {
                    color: (params) => {
                        const idx = params.dataIndex;
                        if (idx > 0 && ohlc[idx][1] > ohlc[idx-1][1]) {
                            return '#ff4d4f';
                        }
                        return '#52c41a';
                    }
                }
            }
        ]
    };
    
    klineChart.setOption(option);
}

function renderSignalsTable(buySignals, sellSignals) {
    const buyTbody = document.getElementById('buy-signals-tbody');
    const sellTbody = document.getElementById('sell-signals-tbody');
    
    buyTbody.innerHTML = '';
    sellTbody.innerHTML = '';
    
    buySignals.forEach(sig => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${sig.date}</td>
            <td class="buy-color">${sig.price.toFixed(2)}</td>
            <td>${sig.reason}</td>
        `;
        buyTbody.appendChild(row);
    });
    
    sellSignals.forEach(sig => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${sig.date}</td>
            <td class="sell-color">${sig.price.toFixed(2)}</td>
            <td>${sig.reason}</td>
        `;
        sellTbody.appendChild(row);
    });
}

function showError(msg) {
    const alertDiv = document.getElementById('alert');
    alertDiv.textContent = msg;
    alertDiv.className = 'alert alert-error';
    alertDiv.style.display = 'block';
    setTimeout(() => { alertDiv.style.display = 'none'; }, 3000);
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

window.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('input, select, textarea').forEach(el => el.setAttribute('autocomplete', 'off'));
    document.getElementById('validate-btn').addEventListener('click', validateFormula);
    initTabs();
    loadUserInfo();
});

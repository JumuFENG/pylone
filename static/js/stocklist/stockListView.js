'use strict';

const showname = {'normal': '普通账户', 'collat': '担保品账户'}

function createLabelWithWidth(t, w) {
    var label = document.createElement('label');
    label.textContent = t;
    label.style.width = w;
    label.style.display = 'inline-block';
    label.style.textAlign = 'center';
    return label;
}

function getStockEarned(stock) {
    let earned = stock.earned??0;
    if (stock.strategies && stock.strategies.strategies) {
        for (const k in stock.strategies.strategies) {
            if (stock.strategies.strategies[k].key != 'StrategyBSBE') {
                continue;
            }
            if (stock.holdCount > 0 && stock.latestPrice) {
                earned = (stock.latestPrice - stock.strategies.strategies[k].guardPrice) * stock.holdCount;
            } else {
                earned = -stock.strategies.strategies[k].guardPrice;
            }
            return earned;
        }
    }
    if (!stock.earned && stock.holdCount > 0 && stock.latestPrice) {
        earned = (stock.latestPrice - stock.holdCost) * stock.holdCount;
    }
    return earned;
}


class StockView {
    constructor(acc, stock) {
        this.acc = acc;
        this.stock = stock;
        this.container = document.createElement('div');
        this.container.classList.add('stock_list_item');
        this.container.id = 'stock_' + stock;
        this.divTitle = document.createElement('div');
        this.divTitle.onclick = e => {
            this.onStockClicked(this.container, this.stock);
        }
        this.container.appendChild(this.divTitle);
        this.showTitle();
    }

    showTitle() {
        const stock = ustocks.stock(this.acc, this.stock);

        let titlehtml = `
        <a href="http://quote.eastmoney.com/concept/${stock.code}.html#fullScreenChart" target="_blank">${stock.name??stock.code.slice(-6)}</a>
        (${stock.code.slice(-6)})
        最新价：<span class="${common.rg_classname(stock.change)}">${stock.latestPrice}
        <span style="font-size: .9em">${(stock.change * 100).toNarrowFixed(2)}%</span></span>
        `
        const account = ustocks.account(this.acc);
        if (!account.realcash) {
            titlehtml += `<button class="btn-outline btn-bdr-danger" title="错误的记录或者实际无法买入的情况需要舍弃." onclick="accld.removeWatch(event, '${this.acc}', '${this.stock}');">舍弃</button>`
        } else if (!stock.holdCount) {
            titlehtml += `<button class="btn-outline btn-bdr-danger" onclick="accld.forgetStock(event, '${this.acc}', '${this.stock}');">删除</button>`
        }
        titlehtml += `<div>
        最新市值: ${(stock.latestPrice * stock.holdCount).toNarrowFixed(2)} 成本:${stock.holdCost} 数量:${stock.holdCount}
        `;
        if (ustocks?.plannedDividen[stock.code]) {
            var pdiv = ustocks.plannedDividen[stock.code];
            detailhtml += `<br><div class="red">${new Date(pdiv.record).toLocaleDateString('zh-cn', {dateStyle:'full'})} ${pdiv.divdesc}</div>`;
        }
        titlehtml += '</div>'
        this.divTitle.innerHTML = titlehtml;
        this.showWarningInTitle();
    }

    refresh() {
    }

    showWarningInTitle() {
        const stock = ustocks.stock(this.acc, this.stock);
        var strGrp = stock.strategies;
        var needfix = false;
        if (strGrp && strGrp.strategies) {
            var strategies = strGrp.strategies;
            if (stock.holdCount > 0) {
                for (const i in strategies) {
                    const str = strategies[i];
                    if (str.enabled && str.key == 'StrategyMA' && str.guardPrice - stock.latestPrice > 0) {
                        needfix = true;
                        break;
                    }
                    if (str.enabled && (str.key.includes('Buy') || (str.kltype !== undefined && str.kltype - 30 < 0))) {
                        needfix =  true;
                        break;
                    }
                }
            }
        }
        if (strGrp && strGrp.buydetail) {
            let tcnt = 0;
            strGrp.buydetail.forEach(bd => {
                tcnt += parseInt(bd.count);
            });
            if (tcnt != stock.holdCount) {
                needfix = true;
            }
        }

        if (needfix) {
            this.divTitle.style.borderLeft = '5px solid red';
            this.divTitle.style.paddingLeft = '10px';
        }
    }

    onStockClicked(target, stk) {}

    removeMe() {}
}


class LostAssignItem {
    constructor(acc, code) {
        this.root = document.createElement('div');
        this.acc = acc;
        this.stock = code;
        this.chkSelecting = document.createElement('input');
        this.chkSelecting.type = 'checkbox';
        this.chkSelecting.value = code;
        this.chkSelecting.onchange = e => {
            if (!e.target.checked && this.lostToAssign.value) {
                this.lostToAssign.value = '';
                this.onAssignChanged();
            }
        }
        const stock = ustocks.stock(acc, code);
        let lblName = createLabelWithWidth('', '180px');
        lblName.appendChild(this.chkSelecting);
        lblName.appendChild(document.createTextNode(stock.name + ' ' + stock.code.slice(-6)));
        this.root.appendChild(lblName);
        this.root.appendChild(createLabelWithWidth(stock.holdCount, '75px'));
        let marketValue = (stock.holdCount * (stock.latestPrice??0)).toNarrowFixed(2);
        this.chkSelecting.checked = marketValue - stock.strategies.amount*1.3 < 0;
        this.root.appendChild(createLabelWithWidth(marketValue, '75px'));
        let earned = getStockEarned(stock);
        this.root.appendChild(createLabelWithWidth(earned.toNarrowFixed(2), '75px'));
        this.lostToAssign = document.createElement('input');
        this.lostToAssign.type = 'number';
        this.lostToAssign.style.width = '80px';
        this.lostToAssign.onchange = e => {
            this.onAssignChanged();
        }
        this.root.appendChild(this.lostToAssign);
    }

    checked() {
        return this.chkSelecting.checked;
    }

    saveAssignedLost() {
        if (!this.lostToAssign.value || isNaN(this.lostToAssign.value)) {
            return;
        }

        const stock = ustocks.stock(this.acc, this.stock);
        for (const k in stock.strategies.strategies) {
            if (stock.strategies.strategies[k].key != 'StrategyBSBE') {
                continue;
            }
            let guardPrice = parseFloat(stock.strategies.strategies[k].guardPrice);
            if (stock.holdCount == 0) {
                guardPrice += Math.abs(this.lostToAssign.value || 0);
            } else {
                guardPrice += Math.abs(this.lostToAssign.value || 0) / stock.holdCount;
            }
            stock.strategies.strategies[k].guardPrice = guardPrice.toNarrowFixed(2);
        }

        accld.saveStrategy(this.acc, this.stock, stock.strategies);
        console.log('assign lost', stock.code, this.lostToAssign.value, stock.strategies);
        this.lostToAssign.value = '';
    }

    onAssignChanged() {}
}


class LostAssignmentView {
    constructor(acc) {
        this.acc = acc;
        this.root = document.createElement('div');
        this.root.classList.add('lost-assign-panel');
        this.items = [];
    }

    initUi(code) {
        common.removeAllChild(this.root);
        this.items = [];
        var info = document.createElement('div');
        info.textContent = '均摊亏损到其它长期跟踪的股票中，用于不得不割肉的情况。';
        this.root.appendChild(info);

        this.root.appendChild(document.createTextNode('当前亏损金额: '));
        this.inputLostAmount = document.createElement('input');
        this.inputLostAmount.type = 'number';
        this.inputLostAmount.placeholder = '亏损金额';
        this.inputLostAmount.style.width = '100px';
        this.root.appendChild(this.inputLostAmount);
        this.btnAssign = document.createElement('button');
        this.btnAssign.className = 'btn-outline btn-bdr1';
        this.btnAssign.textContent = '分配亏损';
        this.btnAssign.onclick = e => {
            if (!this.inputLostAmount.value || isNaN(this.inputLostAmount.value)) {
                alert('请输入正确的亏损金额');
                return;
            }
            this.onAssignLost();
        }
        this.root.appendChild(this.btnAssign);
        var btnCancel = document.createElement('button');
        btnCancel.className = 'btn-outline';
        btnCancel.textContent = '取消';
        btnCancel.onclick = e => {
            this.onClose();
        }
        this.root.appendChild(btnCancel);

        const stock = ustocks.stock(this.acc, code);
        this.inputLostAmount.value = getStockEarned(stock).toNarrowFixed(2);
        this.stockListDiv = document.createElement('div');
        this.root.appendChild(this.stockListDiv);
        let titleDiv = document.createElement('div');
        this.stockListDiv.appendChild(titleDiv);

        let chkAll = document.createElement('input');
        chkAll.type = 'checkbox';
        chkAll.checked = true;
        chkAll.onchange = e => {
            for (const i in this.items) {
                this.items[i].chkSelecting.checked = chkAll.checked;
            }
        }
        let lblAll = createLabelWithWidth('', '180px');
        lblAll.appendChild(chkAll);
        lblAll.appendChild(document.createTextNode('全选'));
        titleDiv.appendChild(lblAll);
        titleDiv.appendChild(createLabelWithWidth('持仓数量', '75px'));
        titleDiv.appendChild(createLabelWithWidth('持仓市值', '75px'));
        titleDiv.appendChild(createLabelWithWidth('当前亏损', '75px'));
        titleDiv.appendChild(createLabelWithWidth('新增亏损', '85px'));

        let stocks = this.getStockList();
        for (const s of stocks) {
            this.addStockItem(s);
        }
        this.summaryDiv = document.createElement('div');
        this.root.appendChild(this.summaryDiv);

        var btnClose = document.createElement('button');
        btnClose.style.margin = '5px 30px';
        btnClose.className = 'btn-outline btn-clr1';
        btnClose.textContent = '确认并关闭';
        btnClose.onclick = e => {
            this.confirmAssign();
            this.onClose();
        }
        this.root.appendChild(btnClose);
    }

    addStockItem(code) {
        const stock = ustocks.stock(this.acc, code);
        if (!stock?.strategies?.strategies) {
            return;
        }

        let strategyMatched = 0;
        for (const k in stock.strategies.strategies) {
            if (stock.strategies.strategies[k].key == 'StrategyBSBE') {
                strategyMatched += 1;
            }
        }
        if (strategyMatched == 0) {
            return;
        }
        var laitem = new LostAssignItem(this.acc, code);
        this.items.push(laitem);
        laitem.onAssignChanged = () => {
            this.updateSummary();
        }
        this.stockListDiv.appendChild(laitem.root);
    }

    onAssignLost() {
        let totalLost = parseFloat(this.inputLostAmount.value);
        let targetCnt = this.items.filter(i => i.checked()).length;
        let lostPer = totalLost / targetCnt;
        for (const i in this.items) {
            if (this.items[i].checked()) {
                this.items[i].lostToAssign.value = lostPer.toNarrowFixed(2);
            }
        }
        this.updateSummary();
    }

    updateSummary() {
        let totalAssigned = 0;
        this.items.forEach(i => {
            if (i.checked()) {
                totalAssigned += parseFloat(i.lostToAssign.value) || 0;
            }
        });
        this.summaryDiv.textContent = '已分配亏损: ' + totalAssigned;
        let unassigned = (parseFloat(this.inputLostAmount.value) || 0) - totalAssigned;
        if (unassigned != 0) {
            this.summaryDiv.textContent += ' 未分配亏损: ' + unassigned;
        }
    }

    confirmAssign() {
        for (const i in this.items) {
            if (!this.items[i].checked() || !this.items[i].lostToAssign.value || isNaN(this.items[i].lostToAssign.value)) {
                continue;
            }
            this.items[i].saveAssignedLost();
        }
    }

    // 以下为回调接口
    getStockList() {
        return [];
    }

    onClose() {}
}


class StockListPanelPage extends RadioAnchorPage {
    constructor(acc, pgcontainer, filt=7) {
        super(acc.nickname??acc.name);
        this.container = pgcontainer;
        this.acc = acc.id;
        this.selected = false;
        this.defaultFilter = filt;
        this.currentCode = null;
        this.orderedCodes = [];
        this.strategyGroupView = new StrategyGroupView();
        let lav = new LostAssignmentView(acc.id);
        lav.onClose = () => {
            this.strategyGroupView.toggleDistributeView(false);
        }
        lav.getStockList = () => {
            return this.orderedCodes;
        }
        this.strategyGroupView.distributeView = lav;
        this.strategyGroupView.root.appendChild(lav.root);
    }

    createContainer() {
    }

    async queryUserStocks() {
        const response = await fetch(`${API_BASE}/stock?act=watchings&accid=${this.acc}`, {
            credentials: 'include'
        });

        if (!response.ok) {
            throw new Error('获取用户持仓失败');
        }

        return await response.json();
    }

    async queryUserDeals() {
        const response = await fetch(`${API_BASE}/stock?act=deals&accid=${this.acc}`, {
            credentials: 'include'
        });

        if (!response.ok) {
            throw new Error('获取用户成交失败');
        }

        return await response.json();
    }

    async show() {
        super.show();
        if (ustocks.account(this.acc).stocks === undefined) {
            let stocks = await this.queryUserStocks();
            let deals = await this.queryUserDeals();
            deals = deals.map( d => {d.type = d.tradeType, d.date = d.time; return d;});
            for (const c in stocks) {
                stocks[c].deals = deals.filter(d => d.code == c);
            }
            this.initUi(stocks);
            if (ustocks.account(this.acc).stocks === undefined) {
                ustocks.account(this.acc).stocks = {};
            }
        } else {
            common.removeAllChild(this.container);
            this.orderedCodes.forEach(code => this.addStock(code));
            if (this.currentCode && this.orderedCodes.includes(this.currentCode)) {
                this.container.querySelector('#stock_' + this.currentCode).firstElementChild.click();
            }
        }
        // if (!emjyBack.costDog) {
        //     var durl = emjyBack.fha.server + 'stock?act=costdog';
        //     fetch(durl, emjyBack.headers).then(r => r.json()).then(cdata => emjyBack.costDog = cdata);
        // }
    }

    hide() {
        super.hide();
        if (this.strategyGroupView) {
            this.strategyGroupView.saveStrategy();
        }
    }

    initUi(stocks) {
        feng.getStockBasics(Object.keys(stocks)).then(sbasic => {
            for (const c in sbasic) {
                stocks[c].code = c;
                stocks[c].name = sbasic[c]?.secu_name;
                stocks[c].latestPrice = sbasic[c]?.last_px;
                Object.assign(stocks[c], sbasic[c]);
            }

            if (this.strategyGroupView.root.parentElement) {
                this.strategyGroupView.root.parentElement.removeChild(this.strategyGroupView.root);
            }
            common.removeAllChild(this.container);
            const account = ustocks.account(this.acc);
            if (account.realcash) {
                stocks = Object.values(stocks).sort((a, b) => {
                    let va = a.holdCount * a.latestPrice;
                    let vb = b.holdCount * b.latestPrice;
                    if (va - vb == 0) {
                        return a.code.substring(2) - b.code.substring(2);
                    }
                    return vb - va;
                });
            } else {
                const fs = Object.values(stocks).filter(s => !s.strategies?.buydetail);
                stocks = Object.values(stocks).filter(s => s.strategies?.buydetail).sort((a, b) => {
                    return a.strategies.buydetail.slice(-1)[0].date - b.strategies.buydetail.slice(-1)[0].date;
                });
                stocks = stocks.concat(fs);
            }
            this.orderedCodes = [];
            for (const s of stocks) {
                if (!account.realcash && (new Date()).getHours() >= 15) {
                    this.fix_date_price(s);
                    if (s.strategies?.buydetail && s.strategies.buydetail.slice(-1)[0].date >= guang.getTodayDate('-')) {
                        const infixing = this.enable_track_strategies(s.strategies.strategies);
                        if (infixing) {
                            s.strategies.infixing = true;
                        }
                    }
                }
                this.orderedCodes.push(s.code);
                ustocks.cache_stock(this.acc, s);

                this.addStock(s.code);
            }
        });
    }

    isBuystrJson(str) {
        return str.key.includes('Buy') || ses.ComplexStrategyKeyNames[str.key];
    }

    isSellstrJson(str) {
        return str.key.includes('Sell') || ses.ComplexStrategyKeyNames[str.key];
    }

    onFiltered(fid) {
        if (!this.orderedCodes) {
            return;
        }
        for (const c of this.orderedCodes) {
            var stocki = ustocks.stock(this.acc, c);
            const containeri = this.container.querySelector('#stock_'+c);
            containeri.style.display = 'none';
            if (fid == 0) { // '持仓'
                if (stocki.holdCount > 0) {
                    containeri.style.display = 'block';
                }
            } else if (fid == 1) { // <安全线
                if (stocki.holdCount > 0) {
                    for (var k in stocki.strategies.strategies) {
                        const str = stocki.strategies.strategies[k];
                        if (str.guardPrice && str.guardPrice - stocki.latestPrice > 0) {
                            containeri.style.display = 'block';
                            break;
                        }
                    }
                }
            } else if (fid == 2) { // 无/误策略
                if (!stocki.strategies || !stocki.strategies.strategies || Object.keys(stocki.strategies.strategies).length == 0 || stocki.strategies.amount - 1000 < 0) {
                    containeri.style.display = 'block';
                    continue;
                }

                var needfix = false;
                if (stocki.holdCount > 0) {
                    var sellstrCount = 0;
                    for (const k in stocki.strategies.strategies) {
                        const str = stocki.strategies.strategies[k];
                        if (this.isSellstrJson(str) && str.enabled) {
                            sellstrCount ++;
                        }
                    }
                    if (sellstrCount == 0) {
                        needfix = true;
                    }
                } else {
                    var buystrCount = 0;
                    for (const k in stocki.strategies.strategies) {
                        const str = stocki.strategies.strategies[k];
                        if (str.enabled && str.key.includes('Sell')) {
                            needfix =  true;
                            break;
                        } else if (str.enabled && this.isBuystrJson(str)) {
                            buystrCount ++;
                        }
                    }
                    if (buystrCount == 0) {
                        needfix = true;
                    }
                }
                if (needfix) {
                    containeri.style.display = 'block';
                    continue;
                }
            } else if (fid == 3) { // 低位横盘, 无持仓
                if (stocki.holdCount == 0) {
                    if (emjyBack.klines[stocki.code] && emjyBack.klines[stocki.code].bottomRegionDays('101') > 15) {
                        if (Object.keys(stocki.strategies.strategies).length > 1) {
                            continue;
                        }
                        if (emjyBack.klines[stocki.code].isWaitingBss('30')) {
                            containeri.style.display = 'block';
                        }
                    }
                }
            } else if (fid == 4) { // 持仓连板
                if (stocki.holdCount > 0 && stocki.latestPrice - stocki.up_price == 0) {
                    containeri.style.display = 'block';
                }
            } else if (fid == 5) { // 无持仓割肉股
                if (stocki.holdCount == 0 && stocki.earned < 0) {
                    containeri.style.display = 'block';
                }
            } else if (fid == 6) { // 盈利清仓股
                if (stocki.holdCount == 0 && stocki.earned > 0) {
                    containeri.style.display = 'block';
                }
            } else if (fid == 7) { // 全部
                containeri.style.display = 'block';
            } else if (fid == 8) { // 今日买入
                if (stocki.strategies && stocki.strategies.buydetail && stocki.strategies.buydetail.length > 0) {
                    var lbd = stocki.strategies.buydetail[0].date;
                    if (stocki.strategies.buydetail.length > 1) {
                        for (const bd of stocki.strategies.buydetail) {
                            if (bd.date > lbd) {
                                lbd = bd.date;
                            }
                        }
                    }
                    if (lbd >= guang.getTodayDate('-')) {
                        containeri.style.display = 'block';
                    }
                }
            } else if (fid == 9) { // 持有时间>1周
                if (stocki.strategies && stocki.strategies.buydetail && stocki.strategies.buydetail.length > 0) {
                    var mbd = stocki.strategies.buydetail[0].date;
                    if (stocki.strategies.buydetail.length > 1) {
                        for (const bd of stocki.strategies.buydetail) {
                            if (bd.date < mbd) {
                                mbd = bd.date;
                            }
                        }
                    }
                    if (new Date(mbd) < new Date(new Date() - 7 * 24 * 60 * 60 * 1000)) {
                        containeri.style.display = 'block';
                    }
                }
            } else if (typeof(fid) === 'string') {
                if (!stocki.strategies) {
                    continue;
                }
                if (accld.getCostDogFilterItems().includes(fid)) {
                    if (stocki.strategies.uramount && stocki.strategies.uramount.key == fid) {
                        containeri.style.display = 'block';
                    }
                } else {
                    for (const k in stocki.strategies.strategies) {
                        const str = stocki.strategies.strategies[k];
                        if (str.key == fid) {
                            containeri.style.display = 'block';
                            break;
                        }
                    }
                }
            }
        }
    }

    onStrategyGroupChanged(code, strGrp) {
        if (!strGrp) {
            return;
        };

        let stk = ustocks.stock(this.acc, code)
        if (!stk) {
            return;
        }
        stk.strategies = strGrp;
    }

    fix_date_price(stock) {
        const records = stock.strategies.buydetail
        if (!records) {
            return;
        }
        for (var rec of records) {
            if (rec.date.includes(':')) {
                var rdate = rec.date.split(' ')[0];
                if (rdate == guang.getTodayDate('-')) {
                    if (rec.date < rdate + ' ' + '09:30') {
                        rec.price = stock.open_px;
                    } else if (rec.date > rdate + ' ' + '14:57') {
                        rec.price = stock.last_px;
                    }
                    rec.date = rdate;
                    stock.strategies.infixing = true;
                } else {
                    console.log('no data to fix date price', rec.date);
                }
            }
        }
    }

    enable_track_strategies (strategies) {
        if (Object.values(strategies).filter(x=>x.enabled).length != 0) {
            return false;
        }
        const interested_strategies = ['StrategyGrid', 'StrategySellELS', 'StrategySellBE'];
        let infixing = false;
        for (var s in strategies) {
            if (interested_strategies.includes(strategies[s].key) && !strategies[s].enabled) {
                strategies[s].enabled = true;
                infixing = true;
            }
        }
        return infixing;
    }

    addStock(code) {
        var divContainer = new StockView(this.acc, code);
        divContainer.onStockClicked = (target, code) => {
            const stk = ustocks.stock(this.acc, code);
            if (!stk) {
                console.log('no stock data', code);
            }
            if (common?.prc_calc && stk) {
                common.prc_calc.init(stk.latestPrice, stk.zdf??guang.getStockZdf(stk.code, stk.name));
            }
            if (this.strategyGroupView && (!this.currentCode || this.currentCode != code)) {
                if (this.strategyGroupView) {
                    this.strategyGroupView.saveStrategy();
                    this.onStrategyGroupChanged(this.currentCode, this.strategyGroupView.strGrp);
                }
                if (this.strategyGroupView.root.parentElement) {
                    this.strategyGroupView.root.parentElement.removeChild(this.strategyGroupView.root);
                }
                target.appendChild(this.strategyGroupView.root);
                this.currentCode = stk.code;
                this.strategyGroupView.latestPrice = stk.latestPrice;
                this.strategyGroupView.initUi(this.acc, stk.code, stk.strategies, stk.deals);
                this.strategyGroupView.toggleDistributeView(false);
            } else if (this.currentCode == code && target !== this.strategyGroupView.root.parentElement) {
                target.appendChild(this.strategyGroupView.root);
            }
        };
        this.container.appendChild(divContainer.container);
        if (!this.orderedCodes.includes(code)) {
            this.orderedCodes.push(code);
        }
    }

    removeStock(code) {
        const con = this.container.querySelector(`#stock_${code}`);
        if (con) {
            con.parentElement.removeChild(con);
        }
        if (this.orderedCodes.includes(code)) {
            this.orderedCodes = this.orderedCodes.filter(c => c != code);
        }
    }
}


const accld = {
    navigator: null,
    addAccount: function(acc) {
        if (!this.navigator) {
            this.navigator = new RadioAnchorBar();
        }
        const slst = document.querySelector('#acc_stock_list');
        const sview = new StockListPanelPage(acc, slst);
        this.navigator.addRadio(sview);
    },
    show: function() {
        this.getPlannedDividen();
        this.initAccFrame();
        this.initWatchArea();
        if (this.navigator.radioAchors.length > 1) {
            document.querySelector('#acc_anchors').appendChild(this.navigator.container);
        }
        this.navigator.selectDefault();
        this.selectionFilter.selectedIndex = 7;
        this.onFiltered(7);
    },
    initAccFrame() {
        this.selectionFilter = document.querySelector('#selection_filter');
        var fitems = this.getFilterItems();
        fitems.forEach(f => {
            this.selectionFilter.options.add(new Option(f));
        });
        this.selectionFilter.onchange = e => {
            this.onFiltered(e.target.selectedIndex);
        }

        this.strategyFilter = document.querySelector('#strategy_filter');
        for (const k in ses.ComplexStrategyKeyNames) {
            this.strategyFilter.options.add(new Option(ses.ComplexStrategyKeyNames[k], k));
        }
        var sepOpt = new Option('------------');
        sepOpt.disabled = true;
        this.strategyFilter.options.add(sepOpt);
        for (const k in ses.BuyStrategyKeyNames) {
            this.strategyFilter.options.add(new Option(ses.BuyStrategyKeyNames[k], k));
        }
        var sepOpt1 = new Option('------------');
        sepOpt1.disabled = true;
        this.strategyFilter.options.add(sepOpt1);
        for (const k in ses.SellStrategyKeyNames) {
            this.strategyFilter.options.add(new Option(ses.SellStrategyKeyNames[k], k));
        }
        this.strategyFilter.onchange = e => {
            this.onFiltered(e.target.value);
        }

        // this.costDogFilter = document.createElement('select');
        // if (!emjyBack.costDog) {
        //     var durl = emjyBack.fha.server + 'stock?act=costdog';
        //     fetch(durl, emjyBack.headers).then(r => r.json()).then(cdata => {
        //         emjyBack.costDog = cdata;
        //     }).then(() => {
        //         this.addCostDogFilterOptions();
        //     });
        // } else {
        //     this.addCostDogFilterOptions();
        // }
        // this.costDogFilter.onchange = e => {
        //     this.onFiltered(e.target.value);
        // }
        // this.container.appendChild(this.costDogFilter);

    },
    getFilterItems() {
        return [
            '持仓',
            '<安全线',
            '无/误策略',
            '低位横盘',
            '持仓连板',
            '割肉',
            '盈利清仓',
            '全部',
            '今日买入',
            '持有>1周'
        ];
    },
    get currentListView() {
        return this.navigator.radioAchors[this.navigator.getHighlighted()];
    },
    newAddedWatchCode: null,
    onFiltered(fid) {
        this.currentListView.onFiltered(fid);
        if (typeof(fid) === 'string') {
            this.selectionFilter.selectedIndex = -1;
            if (this.getCostDogFilterItems().includes(fid)) {
                this.strategyFilter.selectedIndex = -1;
            } else {
                // this.costDogFilter.selectedIndex = -1;
            }
        } else {
            this.strategyFilter.selectedIndex = -1;
            // this.costDogFilter.selectedIndex = -1;
        }
    },
    initWatchAccountSelector() {
        this.watchAccountSelector = document.querySelector('#watch_list_account_selector');
        Object.values(ustocks.accounts).forEach(acc => {
            this.watchAccountSelector.options.add(new Option(acc.nickname??acc.name, acc.id));
        });
        this.watchAccountSelector.options.add(new Option('自动分配', ''));
    },
    initWatchArea() {
        this.inputWatchCode = document.querySelector('#ipt_watch_code');

        var btnAddWatchCode = document.querySelector('#btn_add_watch_code');
        btnAddWatchCode.onclick = e => {
            this.addWatchCode();
        };
    },
    addWatchCode() {
        if (this.inputWatchCode.value.length != 6) {
            alert('Wrong stock code');
            return;
        }
        const code = guang.convertToQtCode(this.inputWatchCode.value);
        feng.getStockBasics(code).then(sbasic => {
            sbasic.strategies = {grptype: "GroupStandard", strategies: {}};
            sbasic.code = code;
            sbasic.name = sbasic.secu_name;
            sbasic.latestPrice = sbasic.last_px;
            sbasic.holdCost = 0;
            sbasic.holdCount = 0;

            ustocks.cache_stock(this.currentListView.acc, sbasic);
            this.currentListView.addStock(code);
            this.newAddedWatchCode = code;
        });
        this.inputWatchCode.value = '';
    },
    getWatchListAccount() {
        return this.watchAccountSelector.value;
    },
    addWatchList() {
        if (!this.inputWatchList.value) {
            return;
        }
        var candidatesObj = JSON.parse(this.inputWatchList.value);
        for(var c in candidatesObj) {
            this.addWatchingStock(guang.convertToQtCode(c), this.getWatchListAccount(), candidatesObj[c]);
        }
        this.inputWatchList.value = '';
    },
    addWatchingStock(code, acc, strGrp) {
        if (acc == '') {
            console.error('can not auto detect rzrq account by now, please select account!');
            return;
        }
        var stock = {code, name:'', holdCount: 0, holdCost: 0};
        stock.strategies = strGrp;
        ustocks.cache_stock(acc, stock);

        this.navigator.radioAchors.forEach(radio => {
            if (radio.acc == acc) {
                radio.addStock(code);
                return;
            }
        });
    },
    getPlannedDividen() {
        var url = `${API_BASE}/stock?act=planeddividen`;
        fetch(url).then(r=>r.json()).then(pdivide => {
            var sdivide = {};
            for (const d of pdivide) {
                var recorddate = d[3];
                var dividedate = d[4];
                var divdesc = d[14];
                sdivide[d[1].substring(2)] = {record: recorddate, divide: dividedate, divdesc};
            }
            ustocks.plannedDividen = sdivide;
        });
    },
    getCostDogFilterItems() {
        // return Object.keys(emjyBack.costDog??{});
        return [];
    },
    addCostDogFilterOptions() {
        var cdItems = this.getCostDogFilterItems();
        for (const ci of cdItems) {
            this.costDogFilter.options.add(new Option(ci));
        }
        this.costDogFilter.selectedIndex = -1;
    },
    removeWatch(evt, acc, code) {
        evt.stopPropagation();
        const rmurl = `${API_BASE}/stock`;
        const stock = ustocks.stock(acc, code);
        const fd = new FormData();
        fd.append('act', 'rmwatch');
        fd.append('code', code);
        fd.append('accid', acc);
        fd.append('buysid', stock.deals.filter(d=>d.tradeType=='B').map(d=>d.sid).join(','));
        fd.append('sellsid', stock.deals.filter(d=>d.tradeType=='S').map(d=>d.sid).join(','));
        fetch(rmurl, {method: 'POST', body: fd, credentials: 'include'});
        const con = document.querySelector(`#stock_${code}`);
        if (con) {
            con.parentElement.removeChild(con);
        }
        ustocks.remove_stock(acc, code);
        this.navigator.radioAchors[this.navigator.getHighlighted()]?.removeStock(code);
    },
    forgetStock(evt, acc, code) {
        evt.stopPropagation();
        const rmurl = `${API_BASE}/stock`;
        const fd = new FormData();
        fd.append('act', 'forget');
        fd.append('code', code);
        fd.append('accid', acc);
        fetch(rmurl, {method: 'POST', body: fd, credentials: 'include'});

        ustocks.remove_stock(acc, code);
        this.navigator.radioAchors[this.navigator.getHighlighted()]?.removeStock(code);
    },
    fixStockDeals(acc, code, data) {
        const url = `${API_BASE}/stock`;
        const fd = new FormData();
        fd.append('act', 'fixdeals');
        fd.append('accid', acc);
        fd.append('code', code);
        fd.append('data', JSON.stringify(data));
        fetch(url, {method: 'POST', body: fd, credentials: 'include'});
    },
    saveStrategy(acc, code, data) {
        if (this.newAddedWatchCode == code) {
            const wurl = `${API_BASE}/stock`;
            const wfd = new FormData();
            wfd.append('act', 'watch');
            wfd.append('accid', acc);
            wfd.append('code', code);
            fetch(wurl, {method: 'POST', body: wfd, credentials: 'include'});
            this.newAddedWatchCode = '';
        }
        const url = `${API_BASE}/stock`;
        const fd = new FormData();
        fd.append('act', 'strategy');
        fd.append('accid', acc);
        fd.append('code', code);
        fd.append('data', JSON.stringify(data));
        fetch(url, {method: 'POST', body: fd, credentials: 'include'});
    }
}

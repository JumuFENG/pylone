class DealsEarningLineChart {
    constructor(cont, title) {
        this.container = cont;
        this.option = {
            legend: {
                left: 'right'
            },
            tooltip: {trigger: 'axis', borderColor: '#ccc', axisPointer: {type: 'cross'}},
            title: {text: title, left: 'center', textStyle: {color: 'rgb(133, 146, 232)'}},
            grid: [{left: '10%', right: '8%', height: '60%'}, {left: '10%', right: '8%', top: '75%', height: '15%'}],
            xAxis: [
                {type: 'category', axisLabel: {show: true}},
                {type: 'category', gridIndex: 1, axisTick: { show: false }, splitLine: { show: false }, axisLabel: { show: false }}
            ],
            axisPointer: {link: {xAxisIndex: 'all'}},
            yAxis: [
                {scale: true},
                {scale: true, gridIndex: 1, axisTick: { show: false }, axisLabel: { show: false }}
            ]
        }
    }

    showDealsByTime(deals, new_tilte=null) {
        for (var dl of deals) {
            if (dl.time.includes(' ')) {
                dl.time = dl.time.split(' ')[0];
            }
        }
        var sortdeals = deals.sort((a,b) => {
            if (a.time == b.time) {
                return a.code > b.code;
            }
            return a.time > b.time;
        });
        var earnings = [];
        var accountStocks = {};
        for (const d of sortdeals) {
            if (d.typebs == 'B') {
                if (!accountStocks[d.code]) {
                    accountStocks[d.code] = {portion: 0, cost: 0, amount: 0};
                }
                accountStocks[d.code].portion += d.portion;
                accountStocks[d.code].cost += d.price * d.portion
                var fee = -d.fee;
                if (d.feeGh !== undefined) {
                    fee -= d.feeGh;
                }
                if (d.feeYh !== undefined) {
                    fee -= d.feeYh;
                }
                fee = -fee;
                if (isNaN(fee)) {
                    fee = 0;
                }
                accountStocks[d.code].cost += fee;
            } else {
                if (!accountStocks[d.code] || accountStocks[d.code].portion < d.portion) {
                    console.error('error sell deal', d);
                    continue;
                }
                accountStocks[d.code].portion -= d.portion;
                var amount = d.price * d.portion;
                var fee = -d.fee;
                if (d.feeGh !== undefined) {
                    fee -= d.feeGh;
                }
                if (d.feeYh !== undefined) {
                    fee -= d.feeYh;
                }
                fee = -fee;
                if (isNaN(fee)) {
                    fee = 0;
                }
                amount -= fee;
                if (accountStocks[d.code].portion == 0) {
                    earnings.push({code: d.code, time: d.time, earn: amount + accountStocks[d.code].amount - accountStocks[d.code].cost});
                    accountStocks[d.code].amount = 0;
                    accountStocks[d.code].cost = 0;
                } else {
                    accountStocks[d.code].amount += amount;
                }
            }
        }
        var dailyEarning = [];
        for (const er of earnings) {
            if (dailyEarning.length == 0 || er.time != dailyEarning[dailyEarning.length - 1].time) {
                dailyEarning.push({time: er.time, earn: er.earn});
            } else {
                dailyEarning[dailyEarning.length - 1].earn += er.earn;
            }
        }
        var tearn = 0;
        dailyEarning.forEach(er => {
            er.earn = Math.round(er.earn, 2);
            tearn += er.earn;
            er.tearn = tearn;
        });

        if (this.chart === undefined) {
            this.chart = echarts.init(this.container);
        }
        this.option.dataset = {source: dailyEarning};
        this.option.series = [
            {
                type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                encode: {x: 'time', y: 'earn'}
            },
            {
                type: 'line', xAxisIndex: 0, yAxisIndex: 0,
                encode: {x: 'time', y: 'tearn'}
            }
        ];
        this.option.dataZoom = {
            show: true
        }
        if (new_tilte) {
            this.option.title.text = new_tilte;
        }
        this.chart.setOption(this.option);
    }
}


const dealcommon = {
    track_deals: {},
    dealsLineChart: null,
    async get_dealcategory() {
        var dcUrl = `${API_BASE}/user/dealcategory`;
        const rsp = await fetch(dcUrl, {credentials: 'include'});
        return rsp.json()??[];
    },
    async get_track_deals(name, realcash=1, accid=null) {
        var url = `${API_BASE}/user/trackdeals?name=${name}&realcash=${realcash}`;
        if (accid) {
            url += '&accid=' + accid;
        }
        const rsp = await fetch(url, {credentials: 'include'}).then(r=>r.json());
        return rsp.deals;
    },
    async show_earing_chart(category, accid, chartdiv, title) {
        if (!this.track_deals[category]) {
            this.track_deals[category] = await this.get_track_deals(category, category == 'archived' ? 1 : 0, accid);
        }

        if (!this.dealsLineChart) {
            this.dealsLineChart = new DealsEarningLineChart(chartdiv, '收益趋势图');
        }
        this.dealsLineChart.showDealsByTime(this.track_deals[category], title + ' 收益曲线');
    }
}

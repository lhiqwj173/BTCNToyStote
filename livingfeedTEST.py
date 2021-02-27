#!/usr/bin/env python
# coding: utf-8
"""
使用说明：
    -安装依赖：
        backtrader
        pysimple-log
        pandas
        requests
        pymongo
        akshare
        easyquotation

    -可选行情来源：
        新浪3s快照：
            1，本地部署mongodb，并开启
            2，按需修改K_record.py livingfeedTEST.py
            3，开一个终端运行K_record.py
            4，运行livingfeedTEST.py

        akshare分钟级数据：
            1，按需修改livingfeedTEST.py
            2，运行livingfeedTEST.py

    -server酱 微信提醒（可选）
        1，申请server酱 key
        2，补充base_func.py文件开头 key = ''# server酱 key
        3，修改相应代码

"""
import backtrader as bt
import datetime
from base_func import send_wechat
from base_func import get_fack_time
from live_data import AStockLivingData

class MA_20_Strategy(bt.Strategy):
    params = (
        ('none', 0),
    )

    def notify_data(self, data, status, *args, **kwargs):
        print('[{}]:{}'.format(data._name, data._getstatusname(status)))
        self._st = data._getstatusname(status)

    def log(self, txt):
        dt = self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self._st = None
        self.mas = dict()
        self.s = dict()
        self.time_list = dict()

        # 遍历所有股票,计算20日均线
        for data in self.datas:
            self.mas[data._name] = bt.ind.SMA(data.close, period=20)
            self.s[data._name] = bt.ind.CrossOver(data.close, self.mas[data._name])
            # self.time_list[data._name] = data._get_time_list()

    def prenext(self):
        # self.log('prenext:{}-{}'.format(self.data0.close[0],self.data0.open[0]))
        self.log('prenext')

    def next(self):
        # 判断状态
        # _NOTIFNAMES = ['START', 'LIVE', 'OVER', 'HISTORBACK']
        if self._st == 'LIVE':
            for data in self.datas:
                # 周期过滤 bt.TimeFrame.Days
                # (Ticks, MicroSeconds, Seconds, Minutes,
                #  Days, Weeks, Months, Years, NoTimeFrame) = range(1, 10)
                if 0 == (len(self) * self.data0._compression) % data._compression:
                    print(len(self))
                    print('===========股票：{}=时间：{}=周期：{}============'.format(data._name,bt.TimeFrame.Names[data._timeframe],data._compression))
                    self.log('[CODE:{}]OPEN:{} HIGH:{} LOW:{} CLOSE:{} VOL:{}'.format(data._name,data.open[0],data.high[0],data.low[0],data.close[0],data.volume[0]))
                    self.log('MA20:{}'.format(self.mas[data._name][0]))
                    self.log('CrossOver:{}'.format(self.s[data._name][0]))
                    if 1 == self.s[data._name][0]:
                        title = data._name
                        content = 'CrossOver:{}'.format(self.s[data._name][0])
                        # send_wechat(title, content)#发送微信提醒
                        print(title, content)

cerebro = bt.Cerebro(oldtrades=False, stdstats=False)
#=======================================================================================================================
#测试(无行情时间段测试运行流程)
#fack_time = get_fack_time(datetime.time(7, 46),5)

#使用新浪快照实时数据
# feed_sh512690 = AStockLivingData('sh512690', 1, fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)
# feed_sh512480 = AStockLivingData('sh512480', 1, fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)
# feed_sh512660 = AStockLivingData('sh512660', 1, fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)

#使用akshare实时数据
# feed_sh512690 = AStockLivingData('sh512690', 1,data_from='akshare', fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)
# feed_sh512480 = AStockLivingData('sh512480', 1,data_from='akshare', fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)
# feed_sh512660 = AStockLivingData('sh512660', 1,data_from='akshare', fack_time=fack_time, timeframe=bt.TimeFrame.Minutes, compression=1)

# cerebro.adddata(feed_sh512690, name='sh512690')
# cerebro.adddata(feed_sh512480, name='sh512480')
# cerebro.adddata(feed_sh512660, name='sh512660')
#
# cerebro.resampledata(feed_sh512690, name='sh512690_5m', timeframe=bt.TimeFrame.Minutes, compression=5)
# cerebro.resampledata(feed_sh512480, name='sh512480_5m', timeframe=bt.TimeFrame.Minutes, compression=5)
# cerebro.resampledata(feed_sh512660, name='sh512660_5m', timeframe=bt.TimeFrame.Minutes, compression=5)

#=======================================================================================================================
#真实数据
#使用新浪快照实时数据
# feed_sh512690 = AStockLivingData('sh512690', timeframe=bt.TimeFrame.Minutes, compression=30)
# feed_sh512480 = AStockLivingData('sh512480', timeframe=bt.TimeFrame.Minutes, compression=30)
# feed_sh512660 = AStockLivingData('sh512660', timeframe=bt.TimeFrame.Minutes, compression=30)

#使用akshare实时数据
# feed_sh512690 = AStockLivingData('sh512690',data_from='akshare', timeframe=bt.TimeFrame.Minutes, compression=5)
# feed_sh512480 = AStockLivingData('sh512480',data_from='akshare', timeframe=bt.TimeFrame.Minutes, compression=5)
# feed_sh512660 = AStockLivingData('sh512660',data_from='akshare', timeframe=bt.TimeFrame.Minutes, compression=5)

#resample K线
# cerebro.adddata(feed_sh512690, name='sh512690_30m')
# cerebro.adddata(feed_sh512480, name='sh512480_30m')
# cerebro.adddata(feed_sh512660, name='sh512660_30m')
#
# cerebro.resampledata(feed_sh512690, name='sh512690_60m', timeframe=bt.TimeFrame.Minutes, compression=60)
# cerebro.resampledata(feed_sh512480, name='sh512480_60m', timeframe=bt.TimeFrame.Minutes, compression=60)
# cerebro.resampledata(feed_sh512660, name='sh512660_60m', timeframe=bt.TimeFrame.Minutes, compression=60)
#
# cerebro.resampledata(feed_sh512690, name='sh512690_120m', timeframe=bt.TimeFrame.Minutes, compression=120)
# cerebro.resampledata(feed_sh512480, name='sh512480_120m', timeframe=bt.TimeFrame.Minutes, compression=120)
# cerebro.resampledata(feed_sh512660, name='sh512660_120m', timeframe=bt.TimeFrame.Minutes, compression=120)
#=======================================================================================================================

cerebro.addstrategy(MA_20_Strategy)

cerebro.broker.setcash(10000.0)

print('Starting Running')

result = cerebro.run()

print('Finish Running')

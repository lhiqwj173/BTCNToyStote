import backtrader as bt
import datetime,time
from datetime import timedelta
import pandas as pd
from backtrader.utils.py3 import queue

from base_func import get_from_akshare
from base_func import stock_zh_a_minute
from base_func import ST_TIME_WAIT, ST_TIME_OPEN, ST_TIME_REST, ST_TIME_CLOSE
from base_func import st_trade

# from base_func import send_wechat #发送微信服务

#TODO 交易时间处理

class AStockLivingData(bt.feed.DataBase):
    """
    :param symbol: 标的代码，格式 sz000001
    
    :param fack_mode: 模拟模式（用于非交易时间测试），随机产生快照数据
        1：按交易时间模拟
        2：无限制时间模拟
    
    :param fack_time: 模拟时间 ,配合fack_mode使用，输入为(开市,午休，开市，闭市)的datetime.datetime时间元组
        fack_time = trade_time_open, trade_time_rest, trade_time_re_open, trade_time_close
    
    :param backfill_bars: 历史数据回填的数量
    
    :param data_from: 实时数据来源
        'snapshot'：新浪3s快照，需配合 K_record（录制行情），需要搭建mongodb ###推荐###
        'ak_share'：akshare的分钟数据，不推荐，数据延迟8秒，容易被封ip
    
    :param source: snapshot来源
        'sina'
        'tencent' 腾讯源好像有问题
    """

    START, LIVE, OVER, HISTORBACK = range(4)

    _NOTIFNAMES = ['START', 'LIVE', 'OVER', 'HISTORBACK']

    source_list = ['sina', 'tencent']

    time_list = None

    def __init__(self, symbol, fack_mode=None,fack_time=None, backfill_bars=240, data_from='snapshot', source='sina', **kwargs):
        super().__init__(**kwargs)

        self.symbol = symbol
        self.backfill_bars = backfill_bars
        self.fack_mode = fack_mode
        self.source = source
        self.data_from = data_from
        self.date = datetime.date.today()
        self.fack_time = fack_time
        self.fack_time_period = ((datetime.datetime.combine(self.date, self.fack_time[1]) - datetime.datetime.combine(self.date, self.fack_time[0])).seconds)/60 if self.fack_time else None


        if 'snapshot' == self.data_from:
            today = self.date.strftime("%Y%m%d")
            collection = '_'.join([str(self.source), today])
            if self.fack_mode:
                collection = ''.join(['[TEST]', collection])
            import pymongo
            database = pymongo.MongoClient("localhost", 27017)['DailyLiveData']
            self.snapshot_col = database[collection]

            backup = [i for i in self.source_list if i != self.source][0]
            self.backup_col = database[collection.replace(self.source,backup)]

        # snapshot2bar时间刻度
        if 2 == self.fack_mode:
            begin_dt_am = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                     '%Y-%m-%d %H:%M')
            self.time_list = [begin_dt_am + timedelta(minutes=(i + 1) * self.p.compression) for i in range((60 * 2) // self.p.compression)]
        elif self.fack_time:
            begin_dt_am = datetime.datetime.combine(self.date, self.fack_time[0])
            begin_dt_pm = datetime.datetime.combine(self.date, self.fack_time[2])
            time_list_am = [begin_dt_am + timedelta(minutes=(i + 1) * self.p.compression) for i in
                            range(int(self.fack_time_period) // self.p.compression)]
            time_list_pm = [begin_dt_pm + timedelta(minutes=(i + 1) * self.p.compression) for i in
                            range(int(self.fack_time_period) // self.p.compression)]
            self.time_list = time_list_am + time_list_pm
        else:
            begin_dt_am = datetime.datetime.combine(self.date, datetime.time(9, 30))
            begin_dt_pm = datetime.datetime.combine(self.date, datetime.time(13))
            time_list_am = [begin_dt_am + timedelta(minutes=(i + 1) * self.p.compression) for i in range((60 * 2) // self.p.compression)]
            time_list_pm = [begin_dt_pm + timedelta(minutes=(i + 1) * self.p.compression) for i in range((60 * 2) // self.p.compression)]
            self.time_list = time_list_am + time_list_pm

        self.time_list.append(begin_dt_am + timedelta(days=1))

    def start(self):
        super().start()
        # add filter adjust time
        self.resample(timeframe=self.p.timeframe, compression=self.p.compression)
        self._state = self.START

    def islive(self):
        return True

    def _load(self):
        if self._state == self.OVER:
            print('[{}]OVER'.format(self.symbol))
            return False
        while True:

            if self._state == self.LIVE:#获取实时数据

                if st_trade(self.fack_mode,type='feed',fack_time=self.fack_time) == ST_TIME_OPEN:

                    now_K_dt = self.get_now_K_dt()

                    self.time_list_temp = [i for i in self.time_list if i > self.num2date(self.lines.datetime[-1])]
                    self.time_list = self.time_list_temp

                    # 是否新BAR
                    if (now_K_dt != self.datetime.datetime(-1) and (now_K_dt in self.time_list)) \
                            or \
                            (self.datetime.datetime(-1) < self.time_list[0] < now_K_dt < self.time_list[1]):
                        # print('prebar时间：{} 时间列表1st：{} 当前时间：{} 时间列表2nd：{}'.format(self.datetime.datetime(-1),self.time_list[0],now_K_dt,self.time_list[1]))

                        # request data
                        if 'snapshot' == self.data_from:
                            df = self.get_K_now_snapshot(self.symbol, now_K_dt,self.p.compression)
                        else:
                            df = self.get_K_now_akshare(self.symbol, now_K_dt,self.p.compression)

                        if df is None:
                            # No more data
                            self._state = self.OVER
                            self.put_notification(self.OVER)
                            title = '[WARNING] {} 未获取到行情数据'.format(self.symbol)
                            content = ''
                            #发送微信提醒
                            # send_wechat(title, content)
                            print(title)
                            return False
                        msg = df.iloc[0]

                        # fill the lines
                        self.lines.datetime[0] = self.date2num(msg['datetime'])
                        self.lines.open[0] = msg['open']
                        self.lines.high[0] = msg['high']
                        self.lines.low[0] = msg['low']
                        self.lines.close[0] = msg['close']
                        self.lines.volume[0] = msg['vol']
                        self.lines.openinterest[0] = -1

                        # Say success
                        self.time_list.pop(0)
                        return True
                    else:
                        continue

                elif st_trade(self.fack_mode,type='feed',fack_time=self.fack_time) == ST_TIME_REST:  # 中午休市，sleep
                    wait_time = datetime.datetime.combine(self.date, datetime.time(13)) - datetime.datetime.now()
                    if self.fack_time:
                        wait_time = datetime.datetime.combine(self.date, self.fack_time[2]) - datetime.datetime.now()

                elif st_trade(self.fack_mode,type='feed',fack_time=self.fack_time) == ST_TIME_WAIT:  # 等待开市，sleep
                    wait_time = datetime.datetime.combine(self.date, datetime.time(9, 25)) - datetime.datetime.now()
                    if self.fack_time:
                        wait_time = datetime.datetime.combine(self.date, self.fack_time[0]) - datetime.datetime.now()

                elif st_trade(self.fack_mode,type='feed',fack_time=self.fack_time) == ST_TIME_CLOSE: # 闭市
                    print('[{}]闭市'.format(self.symbol))
                    self._state = self.OVER
                    self.put_notification(self.OVER)
                    return False

                #sleep 等待开市
                if wait_time:
                    if 0 == wait_time.days:

                        if wait_time.seconds <= 2:
                            wait_time = None
                            continue
                        else:
                            print('[{}]等待时间{}'.format(self.symbol, wait_time.seconds))
                            time.sleep(wait_time.seconds - 2)
                            wait_time = None
                            continue

            if self._state == self.START:#获取历史数据
                self.qhist = self.get_K_pre(self.symbol, self.p.compression, self.backfill_bars)
                self._state = self.HISTORBACK
                self.put_notification(self.HISTORBACK)
                continue

            if self._state == self.HISTORBACK:  #填充历史数据

                if self.qhist.empty():
                    self._state = self.LIVE
                    self.put_notification(self.LIVE)
                    return None

                msg = self.qhist.get()
                if self._load_history(msg):
                    return True

    def get_K_now_snapshot(self,symbol,dt,compression):
        end = dt#datetime
        begin = end - timedelta(minutes=compression)#datetime

        is_first = False
        if begin.time() == datetime.time(9,30) and not self.fack_time and 2 != self.fack_mode:
            # 当日本周期第一个bar，开盘价为集合竞价
            is_first = True

        if self.fack_time:
            if begin.time() == self.fack_time[0]:
                # 当日本周期第一个bar，开盘价为集合竞价
                is_first = True
        # 向前取10秒,以获取前一个成交量
        pre_begin = begin - timedelta(seconds=10)

        if datetime.time(11, 30) < pre_begin.time() < datetime.time(13) and not self.fack_time and 2 != self.fack_mode:
            pre_begin = pre_begin - timedelta(minutes=60 * 2)

        if self.fack_time:
            if self.fack_time[1] < pre_begin.time() < self.fack_time[2]:
                # 当日本周期第一个bar，开盘价为集合竞价
                is_first = True

        if is_first:
            pre_begin = pre_begin - timedelta(seconds=60 * 2)

        # 查询当前周期内的snapshot

        if self.snapshot_col.count_documents({'code': symbol,
                                     "datetime": {"$gt": pre_begin.strftime('%Y-%m-%d %H:%M:%S'), "$lte": end.strftime('%Y-%m-%d %H:%M:%S')}}):

            cursor = self.snapshot_col.find(
                {'code': symbol,
                 "datetime": {"$gt": pre_begin.strftime('%Y-%m-%d %H:%M:%S'), "$lte": end.strftime('%Y-%m-%d %H:%M:%S')}},
                {"_id": 0}, batch_size=10000)
        elif self.snapshot_col.count_documents({'code': symbol, "datetime": {"$gt": pre_begin,
                                                                               "$lte": end}}):
            cursor = self.snapshot_col.find(
                {'code': symbol, "datetime": {"$gt": pre_begin,
                                              "$lte": end}},
                {"_id": 0}, batch_size=10000)
        else:#没有周期范围内的快照 --> 交易没有发生/网络问题没有存储成功
            #用沪深300的指数快照验证是否为网络问题
            if self.snapshot_col.count_documents({'code': 'sh000300',
                                     "datetime": {"$gt": pre_begin.strftime('%Y-%m-%d %H:%M:%S'), "$lte": end.strftime('%Y-%m-%d %H:%M:%S')}}) or self.snapshot_col.count_documents({'code': 'sh000300', "datetime": {"$gt": pre_begin,
                                                                               "$lte": end}}):
                #标的没有行情，涨跌停，停牌
                return None

            else:#网络断开,尝试更换源

                if self.backup_col.count_documents({'code': symbol,
                                                      "datetime": {"$gt": pre_begin.strftime('%Y-%m-%d %H:%M:%S'),
                                                                   "$lte": end.strftime('%Y-%m-%d %H:%M:%S')}}):
                    cursor = self.backup_col.find(
                        {'code': symbol,
                         "datetime": {"$gt": pre_begin.strftime('%Y-%m-%d %H:%M:%S'),
                                      "$lte": end.strftime('%Y-%m-%d %H:%M:%S')}},
                        {"_id": 0}, batch_size=10000)
                elif self.backup_col.count_documents({'code': symbol, "datetime": {"$gt": pre_begin,
                                                                                     "$lte": end}}):
                    cursor = self.backup_col.find(
                        {'code': symbol, "datetime": {"$gt": pre_begin,
                                                      "$lte": end}},
                        {"_id": 0}, batch_size=10000)

                else:# 其他异常
                    return None

        df = pd.DataFrame(list(cursor))

        df['datetime'] = pd.to_datetime(df.datetime)

        df_0 = df.copy()
        if is_first:
            price_deal = df_0.iloc[0]['now']
            vol_deal = df_0.iloc[0]['成交量(手)']
        name = df_0.iloc[0]['name']
        pre_close = df_0.iloc[0]['pre_close']
        # 计算tick真实 成交量(手)
        df_0['vol'] = df_0['成交量(手)'].diff().fillna(df_0['成交量(手)'])

        con1 = df['datetime'] > begin
        con2 = df['datetime'] <= end
        # 所需时间段内数据df
        df_1 = df_0[con1 & con2].copy()

        df_1['open'] = df_1['now']
        df_1['high'] = df_1['now']
        df_1['low'] = df_1['now']
        df_1['close'] = df_1['now']

        # 由tick转为所需周期Bar
        df_k = df_1.resample('{}T'.format(compression), on='datetime', closed='right', label='right').agg(
            {'open': 'first',
             'high': 'max',
             'low': 'min',
             'close': 'last',
             'vol': 'sum'}
        )
        if is_first:  # 集合竞价的开盘价，成交量的处理
            df_k.iloc[0]['open'] = price_deal
            df_k.iloc[0]['vol'] = df_k.iloc[0]['vol'] + vol_deal
        df_k['period'] = compression
        df_k['source'] = self.source
        df_k['code'] = symbol
        df_k['name'] = name
        df_k['pre_close'] = pre_close

        return df_k.reset_index()

    #以bar数来获取相应的k线历史数据
    def get_K_pre(self,symbol,period,num):
        df = get_from_akshare(symbol, period, num)
        q = queue.Queue()
        for index, rows in df.iterrows():
            q.put(rows)
        return q

    # 从akshare获取实时数据
    def get_K_now_akshare(self,symbol,dt,period):
        for i in range(3):#重试3次
            time.sleep(8)

            no_forward_dt = [datetime.datetime.combine(self.date, datetime.time(11, 30)), datetime.datetime.combine(self.date, datetime.time(15))]

            if dt not in no_forward_dt:
                df = stock_zh_a_minute(symbol, period, datalen=2)
            else:
                df = stock_zh_a_minute(symbol, period, datalen=1)

            print(dt)
            print(datetime.datetime.strptime((df.iloc[0]['day']), '%Y-%m-%d %H:%M:%S'))
            print(dt == datetime.datetime.strptime((df.iloc[0]['day']), '%Y-%m-%d %H:%M:%S'))
            if not dt == datetime.datetime.strptime((df.iloc[0]['day']), '%Y-%m-%d %H:%M:%S'):
                time.sleep(1.5)
                continue
            else:
                df["open"] = pd.to_numeric(df["open"], errors='coerce')
                df["high"] = pd.to_numeric(df["high"], errors='coerce')
                df["low"] = pd.to_numeric(df["low"], errors='coerce')
                df["close"] = pd.to_numeric(df["close"], errors='coerce')
                df["vol"] = pd.to_numeric(df["volume"], errors='coerce').astype('int32') / 100
                df['datetime'] = df['day']
                df['datetime'] = pd.to_datetime(df.datetime)

                return df

    def _load_history(self,msg):
        self.lines.datetime[0] = self.date2num(msg['datetime'])
        self.lines.volume[0] = msg['vol']
        self.lines.openinterest[0] = 0.0

        self.lines.open[0] = msg['open']
        self.lines.high[0] = msg['high']
        self.lines.low[0] = msg['low']
        self.lines.close[0] = msg['close']

        return True

    def get_now_K_dt(self):
        nt = datetime.datetime.now().time().strftime('%H:%M') + ':00'
        dt_str = self.date.strftime("%Y-%m-%d ") + nt
        dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        return dt

if __name__ == '__main__':
    # feed = AStockLivingData('sh512690', '2021-02-10', timeframe=bt.TimeFrame.Minutes, compression=1)
    # df = feed.get_K_now('sh512690', datetime.datetime.strptime('2021-02-13 10:18', '%Y-%m-%d %H:%M'), 1)
    df = stock_zh_a_minute('sh512690', 30, datalen=2)
    print(df)

    print(type(datetime.datetime.strptime((df.iloc[0]['day']), '%Y-%m-%d %H:%M:%S')))
    print(datetime.datetime.strptime((df.iloc[1]['day']), '%Y-%m-%d %H:%M:%S') == datetime.datetime(2021,2,25,15))
    print(datetime.datetime.strptime((df.iloc[0]['day']), '%Y-%m-%d %H:%M:%S') == datetime.datetime(2021,2,25,15))
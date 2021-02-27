import easyquotation
import pandas as pd
import datetime
import time
import sys
from datetime import timedelta
import pymongo
from queue import Queue

from base_func import CustomThread
from base_func import mylog
from base_func import ST_TIME_WAIT, ST_TIME_OPEN, ST_TIME_REST, ST_TIME_CLOSE
from base_func import ST_TIME
from base_func import st_trade
from base_func import get_fack_time

log = mylog('K_record').get_logger()

def time_log():
    return datetime.datetime.now().time().strftime('%H:%M:%S')

class GETsnapshot():
    def __init__(self, log, fack_mode,fack_time, source):
        self.fack_mode = fack_mode
        self.source = source
        self.log = log
        self.date = datetime.date.today()
        self.fack_time = fack_time

        #记录新浪源
        if 'sina' == self.source or 'all' == self.source:
            self.sina_ticks = Queue()
            self.sina_data = easyquotation.use('sina')
            self._init(self.sina_data, 'sina')

        # 记录腾讯源
        if 'tencent' == self.source or 'all' == self.source:
            self.tencent_ticks = Queue()
            self.tencent_data = easyquotation.use('tencent')
            self._init(self.tencent_data, 'tencent')

        self.q = Queue()

    def run(self):
        workers = 6
        for i in range(workers):
            t = CustomThread(self.q, daemon=True)
            t.start()

        # snapshot2Mongo
        if 'sina' == self.source or 'all' == self.source:
            self.q.put((self._snapshot2Mongo, self.sina_data, self.sina_ticks))
        if 'tencent' == self.source or 'all' == self.source:
            self.q.put((self._snapshot2Mongo, self.tencent_data, self.tencent_ticks))

        while True:
            if st_trade(self.fack_mode,fack_time=self.fack_time) == ST_TIME_OPEN:  # 是否在交易时间内
                if 'sina' == self.source or 'all' == self.source:
                    self.q.put((self._get_snapshot,self.sina_data, self.sina_ticks))
                if 'tencent' == self.source or 'all' == self.source:
                    self.q.put((self._get_snapshot,self.tencent_data, self.tencent_ticks))
                time.sleep(3)  # 每3秒获得一次全市场快照
                continue

            elif st_trade(self.fack_mode,fack_time=self.fack_time) == ST_TIME_REST:# 中午休市，sleep
                wait_time = datetime.datetime.combine(self.date, datetime.time(13)) - datetime.datetime.now()
                if self.fack_time:
                    wait_time = datetime.datetime.combine(self.date, self.fack_time[2]) - datetime.datetime.now()

            elif st_trade(self.fack_mode,fack_time=self.fack_time) == ST_TIME_WAIT:# 等待开市，sleep
                wait_time = datetime.datetime.combine(self.date, datetime.time(9,25)) - datetime.datetime.now()
                if self.fack_time:
                    wait_time = datetime.datetime.combine(self.date, self.fack_time[0]) - datetime.datetime.now()

            elif st_trade(self.fack_mode,fack_time=self.fack_time) == ST_TIME_CLOSE:# 闭市
                self.log.info('行情结束')
                break

            if wait_time:
                if 0 == wait_time.days:

                    if wait_time.seconds <= 2:
                        wait_time = None
                        continue
                    else:
                        self.log.info('等待时间{}'.format(wait_time.seconds))
                        time.sleep(wait_time.seconds-2)
                        wait_time = None
                        continue

        while not self.q.empty():
            self.log.info("等待线程：{}".format(self.q.qsize()))
        sys.exit()

    def _init(self,s_data,s_name):
        s_data.source = s_name

        today = datetime.datetime.now().strftime("%Y%m%d")
        collection = '_'.join([str(s_data.source),today])
        if self.fack_mode:
            collection = ''.join(['[TEST]',collection])
        database = pymongo.MongoClient("localhost", 27017)['DailyLiveData']
        col = database[collection]

        col_names = database.list_collection_names()
        for i in col_names:
            if '[TEST]' in i:
                database.drop_collection(i)
            elif i != collection:
                database.drop_collection(i)

        if not collection in col_names:#不存在
            col.create_index([
                ('code',1),
                ('datetime',1)
            ], unique=True)

        s_data.database = col

    def _get_snapshot(self,s_data,q_ticks):
        data = s_data.market_snapshot(prefix=True)
        df = pd.DataFrame.from_dict(data).T
        df['pre_close'] = df['close']
        df['code'] = df.index
        if s_data.source == 'sina':
            df['datetime'] = df['date'] + ' ' + df['time']
            df[u'成交量(手)'] = df['turnover'] / 100
        elif s_data.source == 'tencent':
            df[u'成交量(手)'] = df[u'成交量(手)'] / 100
        df = df.loc[:, ['code', 'name', 'datetime', 'now', 'pre_close', '成交量(手)']]

        if self.fack_mode:
            # 伪造时间
            df['datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['datetime'] = pd.to_datetime(df.datetime)
            import random
            df['now'] = df['now'] * random.random()*2
            df[u'成交量(手)'] = df[u'成交量(手)'] + random.randint(1, 1000)
        q_ticks.put(df)

    def _snapshot2Mongo(self,s_data, q_ticks):
        database = s_data.database
        while True:
            snapshot = q_ticks.get()
            data = snapshot.to_dict(orient='records')
            try:
                database.insert_many(data, ordered=False)
            except:
                pass
            self.log.info('{}2MongoDone'.format(s_data.source))

def run_snapshot(fack_mode=None,fack_time=None,source='sina'):#1:按交易时间模拟  #2全时间模拟
    """
        fack_mode:
            1:按交易时间模拟
            2全时间模拟

        fack_time:
            使用 get_fack_time(t,period) 来产生测试时间点
            fack_time = trade_time_open, trade_time_rest, trade_time_re_open, trade_time_close

        source:
            'sina'
            'tencent'
            'all'
    """
    snapshot = GETsnapshot(log, fack_mode,fack_time, source)
    snapshot.run()

if __name__ == '__main__':
    # 测试
    fack_time = get_fack_time(datetime.time(7, 46),5)
    run_snapshot(fack_mode=1,fack_time=fack_time)

    # 真实数据
    # run_snapshot()
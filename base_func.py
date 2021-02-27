import threading
import time
from simplelog import Logger
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging
import os

import json
import requests
import akshare as ak
import pandas as pd
import datetime
from datetime import timedelta

key = ''# server酱 key

# 自定义线程
class CustomThread(threading.Thread):
    def __init__(self, queue, **kwargs):
        super(CustomThread, self).__init__(**kwargs)
        self.__queue = queue

    def run(self):
        while True:
            item = self.__queue.get()
            item[0](*item[1:])
            self.__queue.task_done()

#发送微信
def send_wechat(title,content):
    # title and content must be string.
    sckey = key# server酱 key
    url = 'https://sc.ftqq.com/' + sckey + '.send'
    data = {'text':title,'desp':content}
    result = requests.post(url,data)
    return(result)

class levelFilter(logging.Filter):
    def filter(self, record):
        if record.levelno >= logging.WARNING:
            return False
        return True

class mylog(Logger):
    def __init__(self,
                 log_item,
                 log_path='',
                 fmt=None
                 ):

        self.log_time = time.strftime("%Y_%m_%d")
        self.time_to_del = datetime.datetime.today() + timedelta(days=-5)
        self.log_path = log_path or os.path.dirname(os.path.abspath(__file__))
        self.log_name = self.log_path + self.log_time + '_' + log_item + '.log'

        #删除5日前的log，lock
        for fn in os.listdir(self.log_path):
            if '.log' in fn:
                date = datetime.datetime.strptime(fn[:10], '%Y_%m_%d')
                if date < self.time_to_del:
                    print('[DEL]', date, 'log')
                    os.remove(os.path.join(self.log_path, fn))

            elif '.lock' in fn:
                date = datetime.datetime.strptime(fn[3:13], '%Y_%m_%d')
                if date < self.time_to_del:
                    print('[DEL]', date, 'lock')
                    os.remove(os.path.join(self.log_path, fn))

        if not fmt:
            self.fmt = '[%(asctime)s] %(filename)s->%(funcName)s line:%(lineno)d [%(levelname)s]%(message)s'
        else:
            self.fmt = fmt

        super().__init__(filename=self.log_name,log_fmt=self.fmt)

    def get_logger(self):
        log = logging.getLogger(self.name)
        formatter = logging.Formatter(self.log_fmt)
        # 定制handler ,maxBytes=1024 * 1024 * 100 = 100M
        if not self.filename:
            # 如果没有filename 不做切割就可以了,
            # 也不写入文件
            pass
        else:
            # 判断这个文件是否存在
            if not self.exists():
                self.touch()

            rotate_handler = ConcurrentRotatingFileHandler(filename=self.filename,
                                                           backupCount=self.backup_count,
                                                           maxBytes=self.max_bytes)
            rotate_handler.setFormatter(formatter)
            log.addHandler(rotate_handler)
            log.addFilter(levelFilter())
        return log

ST_TIME_WAIT, ST_TIME_OPEN, ST_TIME_REST, ST_TIME_CLOSE = range(4)
ST_TIME = ('WAIT','OPEN','REST','CLOSE')

def st_trade(fack_mode,type='record',fack_time=None):

    if fack_time:
        trade_time_open = datetime.datetime.combine(datetime.date.today(), fack_time[0])
        trade_time_close = datetime.datetime.combine(datetime.date.today(), fack_time[3])
        trade_time_rest = datetime.datetime.combine(datetime.date.today(), fack_time[1])
        trade_time_re_open = datetime.datetime.combine(datetime.date.today(), fack_time[2])
    else:
        trade_time_open = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 30))
        trade_time_close = datetime.datetime.combine(datetime.date.today(), datetime.time(15, 0))
        trade_time_rest = datetime.datetime.combine(datetime.date.today(), datetime.time(11, 30))
        trade_time_re_open = datetime.datetime.combine(datetime.date.today(), datetime.time(13))

    if 'record' == type:
        trade_time_open = (trade_time_open - timedelta(minutes=5)).time()
        trade_time_close = (trade_time_close + timedelta(minutes=5)).time()
        trade_time_rest = (trade_time_rest + timedelta(minutes=5)).time()
    else:
        trade_time_open = trade_time_open.time()
        trade_time_close = (trade_time_close + timedelta(minutes=1)).time()
        trade_time_rest = (trade_time_rest + timedelta(minutes=1)).time()
    trade_time_re_open = trade_time_re_open.time()

    now_time = datetime.datetime.now().time()
    if fack_mode == 2:
        return ST_TIME_OPEN

    elif (trade_time_open <= now_time <= trade_time_rest) or (trade_time_re_open <= now_time <= trade_time_close):
        return ST_TIME_OPEN

    elif now_time < trade_time_open:
        return ST_TIME_WAIT

    elif trade_time_rest < now_time < trade_time_re_open:
        return ST_TIME_REST

    elif now_time > trade_time_close:
        return ST_TIME_CLOSE

def get_fack_time(t,period):
    #trade_time_open, trade_time_rest, trade_time_re_open, trade_time_close = fack_time
    plus = timedelta(minutes=period)
    dt = datetime.datetime.combine(datetime.date.today(),t)
    b = dt + plus
    c = b + plus
    d = c + plus
    return (t,b.time(),c.time(),d.time())

def get_time_list(fack_mode,fack_time,fack_time_period,period):
    #时间刻度
    todate = datetime.date.today()

    if 2 == fack_mode:
        begin_dt_am = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                 '%Y-%m-%d %H:%M')
        time_list = [begin_dt_am + timedelta(minutes=(i + 1) * period) for i in
                          range((60 * 2) // period)]
    elif fack_time:
        begin_dt_am = datetime.datetime.combine(todate, fack_time[0])
        begin_dt_pm = datetime.datetime.combine(todate, fack_time[2])
        time_list_am = [begin_dt_am + timedelta(minutes=(i + 1) * period) for i in
                        range(int(fack_time_period) // period)]
        time_list_pm = [begin_dt_pm + timedelta(minutes=(i + 1) * period) for i in
                        range(int(fack_time_period) // period)]
        time_list = time_list_am + time_list_pm
    else:
        begin_dt_am = datetime.datetime.combine(todate, datetime.time(9, 30))
        begin_dt_pm = datetime.datetime.combine(todate, datetime.time(13))
        time_list_am = [begin_dt_am + timedelta(minutes=(i + 1) * period) for i in
                        range((60 * 2) // period)]
        time_list_pm = [begin_dt_pm + timedelta(minutes=(i + 1) * period) for i in
                        range((60 * 2) // period)]
        time_list = time_list_am + time_list_pm

    time_list.append(begin_dt_am + timedelta(days=1))
    return time_list

def min_resample(min_data, period=5):
    #判断分钟线起始是否有缺失
    dt = datetime.datetime.strptime(str(min_data.iloc[0]['datetime']), '%Y-%m-%d %H:%M:%S')
    date = dt.date()
    t = dt.time()
    if datetime.time(9,31) < t <= datetime.time(13,1):
        #舍弃上午
        begin = datetime.datetime.combine(date, datetime.time(13))
    elif datetime.time(13,1) < t <= datetime.time(15):
        #舍弃下午
        begin = datetime.datetime.combine(dt + timedelta(days=1), datetime.time(9,30))
    else:
        begin = datetime.datetime.combine(date, datetime.time(9,30))

    min_data = min_data.set_index("datetime").loc[begin:].copy()
    min_data["open"] = pd.to_numeric(min_data["open"], errors='coerce')
    min_data["high"] = pd.to_numeric(min_data["high"], errors='coerce')
    min_data["low"] = pd.to_numeric(min_data["low"], errors='coerce')
    min_data["close"] = pd.to_numeric(min_data["close"], errors='coerce')
    min_data["vol"] = pd.to_numeric(min_data["vol"], errors='coerce').astype('int32')

    #分成早上，下午两个部分
    min_data_morning = min_data.loc[datetime.time(9,30):datetime.time(11,31)].copy()
    min_data_afternoon = min_data.loc[
                         datetime.time(13,00):datetime.time(15,1)].copy()

    res = pd.concat(
        [
            min_data_morning.resample('{}T'.format(period), closed='right', label='right', offset='9h30min').agg(
                {'code': 'first',
                 'open': 'first',
                 'high': 'max',
                 'low': 'min',
                 'close': 'last',
                 'vol': 'sum'}
            ),
            min_data_afternoon.resample('{}T'.format(period), closed='right', label='right', offset='13h').agg(
                {'code': 'first',
                 'open': 'first',
                 'high': 'max',
                 'low': 'min',
                 'close': 'last',
                 'vol': 'sum'}
            )
        ]
    ).sort_index()

    #去除交易时间以外的数据
    res = res.dropna(axis=0, how='any').copy()

    return res

def get_from_akshare(code,period,num):

    #周期数是否能整除120（min）
    if 120%period:
        print('不支持的周期数')
        return None

    from_1 = False
    if not check_period(period):
        period = 1
        from_1 = True
        num = 2000
    df = stock_zh_a_minute(symbol=code, period=period, adjust="qfq", datalen=num)

    df['code'] = code
    df["open"] = pd.to_numeric(df["open"], errors='coerce')
    df["high"] = pd.to_numeric(df["high"], errors='coerce')
    df["low"] = pd.to_numeric(df["low"], errors='coerce')
    df["close"] = pd.to_numeric(df["close"], errors='coerce')
    df["vol"] = pd.to_numeric(df["volume"], errors='coerce').astype('int32') / 100
    df['datetime'] = df['day']
    df['datetime'] = pd.to_datetime(df.datetime)
    df = df.reset_index().loc[:, ['code','datetime','open', 'high', 'low', 'close', 'vol']].copy()

    if from_1:
        df = min_resample(df, period=period)

    if df.iloc[-1]['datetime'] > datetime.datetime.now():
        df = df.iloc[:-1,:].copy()

    df = df.iloc[0-num:,:].copy()

    return df

def check_period(period):
    p = [1, 5, 15, 30, 60]

    if period in p:
        return True
    else:
        return False

def stock_zh_a_minute(
    symbol: str = "sh600751", period: str = "5", adjust: str = "", datalen: int = 2000
) -> pd.DataFrame:
    """
    akshare.stock_zh_a_minute 添加bar数可选
    """
    url = (
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/CN_MarketDataService.getKLineData"
    )
    params = {
        "symbol": symbol,
        "scale": period,
        "datalen": datalen,
    }
    r = requests.get(url, params=params)
    temp_df = pd.DataFrame(json.loads(r.text.split("=(")[1].split(");")[0])).iloc[:, :6]
    try:
        ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
    except:
        return temp_df
    if adjust == "":
        return temp_df

    if adjust == "qfq":
        temp_df[["date", "time"]] = temp_df["day"].str.split(" ", expand=True)
        need_df = temp_df[temp_df["time"] == "15:00:00"]
        need_df.index = pd.to_datetime(need_df["date"])
        stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")

        result_df = stock_zh_a_daily_qfq_df.iloc[-len(need_df):, :]["close"].astype(float) / need_df["close"].astype(float)

        temp_df.index = pd.to_datetime(temp_df["date"])
        merged_df = pd.merge(temp_df, result_df, left_index=True, right_index=True)

        merged_df["open"] = merged_df["open"].astype(float) * merged_df["close_y"]
        merged_df["high"] = merged_df["high"].astype(float) * merged_df["close_y"]
        merged_df["low"] = merged_df["low"].astype(float) * merged_df["close_y"]
        merged_df["close"] = merged_df["close_x"].astype(float) * merged_df["close_y"]
        temp_df = merged_df[["day", "open", "high", "low", "close", "volume"]]
        temp_df.reset_index(drop=True, inplace=True)
        return temp_df
    if adjust == "hfq":
        temp_df[["date", "time"]] = temp_df["day"].str.split(" ", expand=True)
        need_df = temp_df[temp_df["time"] == "15:00:00"]
        need_df.index = pd.to_datetime(need_df["date"])
        stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol=symbol, adjust="hfq")
        result_df = stock_zh_a_daily_qfq_df.iloc[-len(need_df):, :]["close"].astype(
            float
        ) / need_df["close"].astype(float)
        temp_df.index = pd.to_datetime(temp_df["date"])
        merged_df = pd.merge(temp_df, result_df, left_index=True, right_index=True)
        merged_df["open"] = merged_df["open"].astype(float) * merged_df["close_y"]
        merged_df["high"] = merged_df["high"].astype(float) * merged_df["close_y"]
        merged_df["low"] = merged_df["low"].astype(float) * merged_df["close_y"]
        merged_df["close"] = merged_df["close_x"].astype(float) * merged_df["close_y"]
        temp_df = merged_df[["day", "open", "high", "low", "close", "volume"]]
        temp_df.reset_index(drop=True, inplace=True)
        return temp_df

if __name__ == '__main__':
    df = stock_zh_a_minute(symbol='sh600751', period='30', adjust="qfq", datalen=1)
    print(df)

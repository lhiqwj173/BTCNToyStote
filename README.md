# BTCNToyStote #
backtrader相关小工具

## backtrader 实时数据源 ##

+ 数据来源（向以下项目的作者表示衷心的感谢）：
  + akshare
  + easyquotation

+ 依赖：
  + backtrader
  + pysimple-log
  + pandas
  + requests
  + pymongo
  + akshare
  + easyquotation

+ 可选行情来源：
  + 新浪3s快照（推荐）：
    1. 本地部署mongodb，并开启
    2. 按需修改K_record.py livingfeedTEST.py
    3. 开一个终端运行K_record.py
    4. 运行livingfeedTEST.py

  + akshare分钟级数据（行情延迟，容易封IP）：
    1. 按需修改livingfeedTEST.py
    2. 运行livingfeedTEST.py

+ server酱 微信提醒（可选）
    1. 申请server酱 key
    2. 补充base_func.py文件开头 key = ''# server酱 key
    3. 修改相应代码

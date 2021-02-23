# -*- coding: utf-8 -*-
import redis
import pandas as pd
import datetime
from ast import literal_eval

r = redis.Redis(host="", port=, password="")
dte = int(datetime.datetime.today().strftime('%Y%m%d'))

key_value_dict = {
    "DCL5Q_901_FUND": r"E:\matic\downloads\666800008221_FUND.{}.csv".format(dte),
    "DCL5Q_901_POSITION": r"E:\matic\downloads\666800008221_POSITION.{}.csv".format(dte),
    "DCL5Q_901_DETAIL": r"E:\matic\downloads\666800008221_DEAL.{}.csv".format(dte),
    "DCL5Q_901_ENTRUST": r"E:\matic\downloads\666800008221_ENTRUST.{}.csv".format(dte),
    
    "DCL5Q_902_COMPACTDEAL": r"E:\matic\downloads\933010000141_RZRQ_COMPACTDEAL.{}.csv".format(dte),
    "DCL5Q_902_POSITION": r"E:\matic\downloads\933010000141_RZRQ_POSITION.{}.csv".format(dte),
    "DCL5Q_902_FUND": r"E:\matic\downloads\933010000141_RZRQ_FUND.{}.csv".format(dte),
    "DCL5Q_902_DETAIL": r"E:\matic\downloads\933010000141_RZRQ_DEAL.{}.csv".format(dte),
    "DCL5Q_902_COMPACT": r"E:\matic\downloads\933010000141_RZRQ_COMPACT.{}.csv".format(dte),
    "DCL5Q_902_ENABLEAMOUNT": r"E:\matic\downloads\933010000141_RZRQ_ENABLEAMOUNT.{}.csv".format(dte),
    "DCL5Q_902_ENTRUST": r"E:\matic\downloads\933010000141_RZRQ_ENTRUST.{}.csv".format(dte),
}


for key, value in key_value_dict.items():
    try:
        res_bytes = r.get(key)
        res_str = res_bytes.decode("utf-8")
        res_dict = eval(res_str)
        res_df = pd.DataFrame(res_dict)
        res_df.to_csv(value, index=False)
        print("get {} key success".format(key))
    except Exception, e:
        print("no {} in redis {}".format(key, e))


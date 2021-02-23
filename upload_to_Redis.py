# -*- coding: utf-8 -*-
import redis, datetime
import pandas as pd
from ast import literal_eval
r = redis.Redis(host="", port=, password="")

dte = int(datetime.datetime.today().strftime('%Y%m%d'))

file_path_dict = {
    "DCL5Q_901": {
        "FUND": r"D:\matic\downloads\666800008221_FUND.{}.csv".format(dte),
        "POSITION": r"D:\matic\downloads\666800008221_POSITION.{}.csv".format(dte),
        "DETAIL": r"D:\matic\downloads\666800008221_DEAL.{}.csv".format(dte),
        "ENTRUST": r"D:\matic\downloads\666800008221_ENTRUST.{}.csv".format(dte)
    },
    "DCL5Q_902": {
        "COMPACT": r"D:\matic\downloads\933010000141_RZRQ_COMPACT.{}.csv".format(dte),
        "COMPACTDEAL": r"D:\matic\downloads\933010000141_RZRQ_COMPACTDEAL.{}.csv".format(dte),
        "ENABLEAMOUNT": r"D:\matic\downloads\933010000141_RZRQ_ENABLEAMOUNT.{}.csv".format(dte),
        "FUND": r"D:\matic\downloads\933010000141_RZRQ_FUND.{}.csv".format(dte),
        "POSITION": r"D:\matic\downloads\933010000141_RZRQ_POSITION.{}.csv".format(dte),
        "DETAIL": r"D:\matic\downloads\933010000141_RZRQ_DEAL.{}.csv".format(dte),
        "ENTRUST": r"D:\matic\downloads\933010000141_RZRQ_ENTRUST.{}.csv".format(dte)
    }
}


for prod, path_dict in file_path_dict.items():
    for file_type, path in path_dict.items():
        key = prod + "_" + file_type
        print(path)
        df = pd.read_csv(path, error_bad_lines=False)
        df = df.fillna("")
        dic = df.to_dict()
        str_dic = str(dic)
        r.set(key, str_dic)
        print("set {} key success".format(key))


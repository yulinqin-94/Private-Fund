# -*- coding: utf-8 -*-
import redis, datetime
import pandas as pd
from ast import literal_eval
r = redis.Redis(host="", port=, password="")

dte = int(datetime.datetime.today().strftime('%Y%m%d'))

file_path_dict = {
    "key": {
        "FUND": r"D:\xxx.{}.csv".format(dte),
        "POSITION": r"D:\xxx.{}.csv".format(dte),
        "DETAIL": r"D:\xxx.{}.csv".format(dte),
        "ENTRUST": r"D:\xxx.{}.csv".format(dte)
    },
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


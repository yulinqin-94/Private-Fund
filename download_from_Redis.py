# -*- coding: utf-8 -*-
import redis
import pandas as pd
import datetime
from ast import literal_eval

r = redis.Redis(host="", port=, password="")
dte = int(datetime.datetime.today().strftime('%Y%m%d'))

key_value_dict = {
    "key1": r"E:\xxx.{}.csv".format(dte),
    "key2": r"E:\xxx.{}.csv".format(dte),
    "key3": r"E:\xxx.{}.csv".format(dte),
    "key4": r"E:\xxx.{}.csv".format(dte),
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


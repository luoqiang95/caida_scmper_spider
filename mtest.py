# -*- coding:utf-8 -*-
# Author: lq
# data:  10:40 AM
# file: mtest.py
import os
import pandas as pd
import numpy as np
from datetime import datetime

t = pd.date_range('20070913', '20071201')
print('20070918' in t)
df = pd.DataFrame(
    {
        "a": "123",
        "b": "456"
    }
    , index=[0])

df.loc[df.a == "123", "b"] = "789"
print(df)
df["a"] = "abc"
print(df)

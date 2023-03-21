import pandas as pd

a = pd.DataFrame({
    "a": 1,
    "b": 2,
    "c": None
}, index=[1])
b = pd.DataFrame({
    "a": 1,
    "b": 3,
    "c": 4
}, index=[1])
i = a.loc[a.a == 1, "c"].values
c = a[a["c"].isin(i)].index
print(c)
print(a.drop(c,axis=0))

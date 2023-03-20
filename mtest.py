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
# c = pd.merge(a, b, on=["a"])
c = a.append(b)
print(c)
# print("=" * 5)
# print(c.drop_duplicates(subset=["a"], keep="first"))
index = c.loc[c.b == 3].index
print(index)
print(c.drop(index, axis=0))

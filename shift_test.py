import numpy as np
import modin.pandas as pd

df1 = pd.DataFrame({"column": [11, 22, 33, 44, 55]}, index=[2, 3, 4, 5, 6])
print(df1)
print(df1.index)
print(df1.columns)

df2 = df1.shift(0)
print("df1 is df2 = ", df1 is df2)
print(df2)
print(df2.index)
print(df2.columns)

s1 = df1["column"]
s2 = s1.shift(0)
print("s1 is s2 = ", s1 is s2)
print(s2)
print(s2.index)

df2 = df1.shift(-2)
print("df1 is df2 = ", df1 is df2)
print(df2)
print(df2.index)
print(df2.columns)

s1 = df1["column"]
s2 = s1.shift(-2)
print("s1 is s2 = ", s1 is s2)
print(s2)
print(s2.index)

print("\n-----------------------------------\n")

df1 = pd.DataFrame({"column": [11, 22, 33, 44, 55]})
print(df1)
print(df1.index)
print(df1.columns)

df2 = df1.shift(0)
print("df1 is df2 = ", df1 is df2)
print(df2)
print(df2.index)
print(df2.columns)

s1 = df1["column"]
s2 = s1.shift(0)
print("s1 is s2 = ", s1 is s2)
print(s2)
print(s2.index)

df2 = df1.shift(-2)
print("df1 is df2 = ", df1 is df2)
print(df2)
print(df2.index)
print(df2.columns)

s1 = df1["column"]
s2 = s1.shift(-2)
print("s1 is s2 = ", s1 is s2)
print(s2)
print(s2.index)

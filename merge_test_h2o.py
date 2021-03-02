import os
import gc
import timeit
import modin.pandas as pd
#import pandas as pd

df1 = pd.read_csv("df1.csv", dtype={'id4': 'category', 'id5': 'category', 'id6': 'category'})
df2 = pd.read_csv("df2.csv", dtype={'id4': 'category', 'id5': 'category'})

print("df1 = \n", df1)
print("df2 = \n", df2)
df3 = df1.merge(df2, on='id2')
print(df3.shape, flush=True)
chk = [df3['v1'].sum(), df3['v2'].sum()]
print("chk = ", chk)
pd.set_option("display.max_rows", 1000000, "display.max_columns", 1000000)
print(df3, flush=True)

#import pandas as pd
import modin.pandas as pd

x = pd.read_csv("G1_1e4_1e2_0_0.csv", dtype={'id1':'category', 'id2':'category', 'id3':'category'})
gb = x[['id2','id4','v1','v2']].groupby(['id2','id4'], observed=True)
print(gb)
print("groups = ", gb.groups)
print("len(groups) = ", len(gb.groups))
print("indices = ", gb.indices)

df = gb.apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
print(type(df))
print(df)

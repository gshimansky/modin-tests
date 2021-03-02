#import pandas as pd
import modin.pandas as pd

x = pd.DataFrame(
    {"id2": ["id056", "id075", "id077", "id072", "id010"],
     "id4": [82, 30, 40, 92, 34],
     "v1": [4, 1, 5, 1, 5],
     "v2": [3, 2, 3, 3, 2]
    }
)
gb = x.groupby(['id2','id4'], observed=True)
print(gb)
print("groups = ", gb.groups)
print("len(groups) = ", len(gb.groups))
print("indices = ", gb.indices)

df = gb.agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
df.reset_index(inplace=True)
print(type(df))
print(df)

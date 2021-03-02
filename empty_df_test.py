import numpy as np
#import pandas as pd
import modin.pandas as pd

df = pd.DataFrame({'id': [], 'max_speed': [], 'health': []})
#df = pd.DataFrame(columns=['id', 'max_speed', 'health'])
print(df)

a = np.array(['one', 'two'])
print(type(a))

df['aaa'] = a
df['id'] = a
print(df)
s = pd.Series(a)
df['bbb'] = s
df['max_speed'] = s
print(df)

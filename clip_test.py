import modin.pandas as pd

data = [x for x in range(256)]
s1 = pd.Series(data)
# Make object partly existent
s2 = s1[:2].reindex(s1.index)
s3 = s2.clip(data, data, axis=0)
print(s3)

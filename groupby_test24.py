import modin.pandas as pd

s1 = pd.Series([10, 10, 10, 1, 1, 1, 2, 3])
s1.name = "data"

print(s1)
print(type(s1))
gb = s1.groupby(level=0)
print(gb.groups)
s2 = gb.agg(result=("max"))
print(s2)
print(type(s2))

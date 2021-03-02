import modin.pandas as pd

df = pd.DataFrame({'col1': ['Marc', 'Lori'], 'col2': [1, 1]})
df.astype({'col1': 'category'}, copy=False)
grpby = df.groupby(by=['col1'], as_index=True)

print(grpby.groups)
print(grpby.indices)

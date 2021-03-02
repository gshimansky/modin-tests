import modin.pandas as pd

df = pd.read_csv("test_quotes_1.txt", sep='\t')
print(df)

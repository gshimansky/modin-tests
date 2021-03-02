import numpy as np
import pandas as pd

n = 10000000
df = pd.DataFrame({'A': np.random.ranf(n), 'B': np.random.ranf(n)})
df.to_csv("test.csv")


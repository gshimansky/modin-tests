import pandas as pd
import numpy as np

RAND_LOW = 0
RAND_HIGH = 100
NCOLS = 2 ** 5
NROWS = 2 ** 2

pd.show_versions()

random_state = np.random.RandomState(seed=42)
data = {
    "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): random_state.randint(
        RAND_LOW, RAND_HIGH, size=(NROWS)
    )
    for i in range(NCOLS)
}
df1 = pd.DataFrame(data)
print(df1.shape)
print(df1)
df2 = df1.sort_values(df1.index[0], axis=1, ignore_index=True)
print(df2.shape)
print(df2)

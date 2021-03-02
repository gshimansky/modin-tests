import numpy as np
import pandas as pd

RAND_LOW = 0
RAND_HIGH = 10
NCOLS = 4
NROWS = 4

random_state = np.random.RandomState(seed=42)
data = {
    "col{}".format(i): random_state.randint(RAND_LOW, RAND_HIGH, size=(NROWS))
    for i in range(NCOLS)
}
df = pd.DataFrame(data)

print(df.shape)
gb = df.groupby(["col3", "col2"], as_index=False)
result = gb.agg(min)
string = "%s" % repr(result)
print(string)

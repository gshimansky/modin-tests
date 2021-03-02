import os
import time
import numpy as np
import modin.pandas as pd

RAND_LOW = 0
RAND_HIGH = 100
NCOLS = 20
NROWS = 100

random_state = np.random.RandomState(seed=42)

data = pd.DataFrame({
        "col{}".format(i): random_state.randint(
            RAND_LOW, RAND_HIGH, size=(NROWS)
        )
        for i in range(NCOLS)
})

pd.set_option("display.max_rows", None, "display.max_columns", None)
print(data)

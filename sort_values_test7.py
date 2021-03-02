import pandas
import modin.pandas as pd
import numpy as np

NCOLS=64
NROWS=256
RAND_LOW = 0
RAND_HIGH = 100
random_state = np.random.RandomState(seed=42)

data = {
    "col{}".format(int((i - NCOLS / 2) % NCOLS + 1)): random_state.randint(
        RAND_LOW, RAND_HIGH, size=(NROWS)
    )
    for i in range(NCOLS)
}

pdf1 = pandas.DataFrame(data)
mdf1 = pd.DataFrame(data)
by = list(data.keys())[0]
print(pdf1)
pdf2 = pdf1.sort_values(by, ascending=True)
print("PANDAS = \n", pdf2)
mdf2 = mdf1.sort_values(by, ascending=True)
print("MODIN = \n", mdf2)

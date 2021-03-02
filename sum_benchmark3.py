import os
import time
import numpy as np
#import ray
#import modin.pandas as pd
import pandas as pd

RAND_LOW = 0
RAND_HIGH = 100
NCOLS = 10000
NROWS = 10000

random_state = np.random.RandomState(seed=42)

def init_data():
    return pd.DataFrame({
            "col{}".format(i): random_state.randint(
                RAND_LOW, RAND_HIGH, size=(NROWS)
            )
            for i in range(NCOLS)
    })

def test3_function(data, iterations):
    region_start = time.time()
    for iii in range(0, iterations):
        s = data.agg("sum")
        string = "%s" % repr(s)
        #print(string)
    region_end = time.time()
    duration = region_end - region_start

    return duration

data = init_data()

duration = test3_function(data, 1)
print("Test3 1 iteration duration = ", duration)

duration = test3_function(data, 10)
print("Test3 10 iterations duration = ", duration)

#duration = test3_function(data, 100)
#print("Test3 100 iterations duration = ", duration)

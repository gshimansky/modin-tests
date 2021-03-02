import os
import time
import numpy as np
#import pandas as pd
import ray
ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman")
import modin.pandas as pd

RAND_LOW = 0
RAND_HIGH = 100000
# 10x
#NCOLS = 5000
#NROWS = 1_000_00
# 3x
NCOLS = 10
NROWS = 100_000_00
ITERATIONS = 1

random_state = np.random.RandomState(seed=42)

data = pd.DataFrame({
        "col{}".format(i): random_state.randint(
            RAND_LOW, RAND_HIGH, size=(NROWS)
        )
        for i in range(NCOLS)
})
data["gb_col"] = ["str_{}".format(random_state.randint(RAND_LOW, RAND_HIGH)) for i in range(NROWS)]

def test_function(data):
    region_start = time.time()
    gb1 = data.groupby("gb_col")
    region_end = time.time()
    duration1 = region_end - region_start
    print("Region 1 duration = ", duration1)

    region_start = time.time()
    gb2 = gb1[data.columns[1]]
    region_end = time.time()
    duration2 = region_end - region_start
    print("Region 2 duration = ", duration2)

    region_start = time.time()
    result = gb2.sum()
    region_end = time.time()
    duration3 = region_end - region_start
    print("Region 3 duration = ", duration3)

    return duration1 + duration2 + duration3

duration = 0
for i in range(0, ITERATIONS):
    duration += test_function(data)
print("Test duration = ", duration)

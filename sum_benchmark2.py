import os
import sys
import argparse
import time
import numpy as np
#import ray
#ray.init(huge_pages=False, plasma_directory="/localdisk/gashiman")
import modin.pandas as pd
#import pandas as pd

RAND_LOW = 0
RAND_HIGH = 100
#NCOLS = 100_000
#NROWS = 100_000
NCOLS = 1_000
NROWS = 1_000

random_state = np.random.RandomState(seed=42)

parser = argparse.ArgumentParser(
    description="Run test with specified number of operations. Results are output into stderr which may be redirected into a separate CVS file."
)
parser.add_argument("-np", type=int, dest="num_part", required=True, help="Number of partitions")
args = parser.parse_args()

def init_data():
    return pd.DataFrame({
            "col{}".format(i): random_state.randint(
                RAND_LOW, RAND_HIGH, size=(NROWS)
            )
            for i in range(NCOLS)
    })

def test2_function(data, iterations):
    region_start = time.time()
    for iii in range(0, iterations):
        s = data.agg(sum)
        string = "%s" % repr(s)
        #print(string)
    region_end = time.time()
    duration = region_end - region_start

    return duration

pd.DEFAULT_NPARTITIONS = args.num_part
print("Partitions number =", args.num_part)

data = init_data()

duration11 = test2_function(data, 1)
print("Test2 1 iteration initial duration = ", duration11)

duration12 = test2_function(data, 1)
print("Test2 1 iteration second duration = ", duration12)

duration10 = test2_function(data, 10)
print("Test2 10 iterations duration = ", duration10)

print("{},{},{},{}".format(args.num_part, duration11, duration12, duration10), file=sys.stderr)

#duration = test2_function(data, 100)
#print("Test2 100 iterations duration = ", duration)

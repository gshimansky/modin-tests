import os
import time
import numpy as np
import pandas as pd
import sklearn.cluster

RAND_LOW = 0
RAND_HIGH = 100
NCOLS = 10
NROWS = 40
NUM_CLUSTERS = 2
ITERATIONS = 100

random_state = np.random.RandomState(seed=42)

data = pd.DataFrame({
        "col{}".format(i): random_state.randint(
            RAND_LOW, RAND_HIGH, size=(NROWS)
        )
        for i in range(NCOLS)
})

def test_function(data):
    region_start = time.time()
    kmean = sklearn.cluster.KMeans(n_clusters=NUM_CLUSTERS, n_init=ITERATIONS, max_iter=3000000,
                                   init='k-means++', random_state=0)
    region_end = time.time()
    duration1 = region_end - region_start
    print("Region 1 duration = ", duration1)

    region_start = time.time()
    distance = pd.DataFrame(kmean.fit_transform(data))
    region_end = time.time()
    duration2 = region_end - region_start
    print("Region 2 duration = ", duration2)

    return duration1 + duration2

duration = test_function(data)
print("Test duration = ", duration)

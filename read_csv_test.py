#import pandas as pd
import ray
ray.init(huge_pages=True, plasma_directory="/mnt/hugepages")
import modin.pandas as pd

dtypes = {
    'object_id': 'int32',
    'mjd': 'float32',
    'passband': 'int32',
    'flux': 'float32',
    'flux_err': 'float32',
    'detected': 'int32'
}

PATH='/localdisk/benchmark_datasets/plasticc'
GPU_MEMORY = 16
TEST_ROWS = 453653104
OVERHEAD = 1.2
SKIP_ROWS = int((1 - GPU_MEMORY/(32.0*OVERHEAD))*TEST_ROWS)

test = pd.read_csv('%s/test_set.csv'%PATH,
        # skiprows=range(1, 1+SKIP_ROWS),
        dtype=dtypes)

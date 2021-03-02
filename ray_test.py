import ray
ray.init()

a = ray.put(1)

@ray.remote
def f(a, b):
    return a + b

ray.get(f.remote(a, a))

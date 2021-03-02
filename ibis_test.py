import ibis
import time

conn = ibis.omniscidb.connect(host="localhost", port=6274,
                              user="admin",
                              password="HyperInteractive")

db = conn.database("ibis_testing")
t0 = time.time()
df = db.table("test").execute()
print("df type = ", type(df))
print("db.table time = ", time.time() - t0)

t1 = time.time()
col = db.table("test").get_column("a").execute()
print("col type = ", type(col))
print("col.table time = ", time.time() - t1)


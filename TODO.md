1. What does Dispatch do? Seems it only calls on local servers. 
    * To collect an initial dep graph? To reduce the rate of conflict?



2. Understand the txn data structure. 

3. How to grab the MDB lock data. (see the OCC implementation)

1. inn_id= piece type  (see workloads) 

Try search
```c++

#define RW_BENCHMARK_W_TXN  (100)
#define RW_BENCHMARK_R_TXN  (200)
#define RW_BENCHMARK_W_TXN_NAME  "WRITE"
#define RW_BENCHMARK_R_TXN_NAME  "READ"

#define RW_BENCHMARK_W_TXN_0 (101)
#define RW_BENCHMARK_R_TXN_0 (201)


```
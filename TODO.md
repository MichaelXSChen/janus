1. What does Dispatch do? Seems it only calls on local servers. 
    * To collect an initial dep graph? To reduce the rate of conflict?
    * What does Janus 


2. Understand the txn data structure. 

3. How to grab the MDB lock data. (see the OCC implementation)

# Code Reading notes

1. inn_id= piece type  (see workloads) Try search
```c++
#define RW_BENCHMARK_W_TXN  (100)
#define RW_BENCHMARK_R_TXN  (200)
#define RW_BENCHMARK_W_TXN_NAME  "WRITE"
#define RW_BENCHMARK_R_TXN_NAME  "READ"

#define RW_BENCHMARK_W_TXN_0 (101)
#define RW_BENCHMARK_R_TXN_0 (201)
```
1. About Dispatch. 
    * In 2PL, the logic is Dispatch->Prepare->Commit. 
    * In Tapir, actually did nothing but created the transaction. 
    * In  
    
1. Qeustion: Do I need to call dispatch??
    * Synchronosly within DC, async without DC
    
1. How to check the timestamp of each row. 
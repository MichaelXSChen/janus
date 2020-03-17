# About Transaction: 

* `TxnPieceData = Simple Command`
* `row_cotext_id` is used to save queried `row*` if the txn use the same primary_keys, so that no need to redo the search. 
* About piece's hint_flag
   * Bypass: bypass trace dep
   * instant: execute instantly, without waiting for commit. 
   * Deferred: wait until commit to execute. 
```
#define TXN_BYPASS   (0x01)
#define TXN_SAFE     (0x02)
#define TXN_INSTANT  (0x02)
#define TXN_DEFERRED (0x04)
```
|          | Execute Before Commit | Execute After Commit | TraceDep |
|----------|-----------------------|----------------------|----------|
| Bypass   | Yes                   | Yes                  | No       |
| Safe     |                       |                      |          |
| Instant  | Yes                   | No                   | Yes      |
| Deferred | No                    | Yes                  | Yes      |

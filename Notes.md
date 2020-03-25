1. In the dispatch phase, the coordinator dispatches the ''ready'' pieces to nearest shards, let the shards to pre-execute them,
get the outputs, so that the coordinator can pop-up the inputs of remaining pieces.
The dispatch process is run recursively.    
    * In other words, the dispatch is also part of the CC. 
2. Problem: The current implementation badly supports NF-Replication. 
    * It does not matter for full replication, as the non-43333333333
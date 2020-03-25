1. In the dispatch phase, the coordinator dispatches the ''ready'' pieces to nearest shards, let the shards to pre-execute them,
get the outputs, so that the coordinator can pop-up the inputs of remaining pieces.
The dispatch process is run recursively.   
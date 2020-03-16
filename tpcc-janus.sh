make && ./run_all.py -hh config/cluster.yml -s '2' -c '1' -r '3' -cc config/tpcc.yml -cc config/client_closed.yml -cc config/brq.yml -b tpcc -m janus:janus --allow-client-overlap simple

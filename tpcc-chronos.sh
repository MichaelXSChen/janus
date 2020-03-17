make && ./run_all.py -hh config/cluster.yml -s '2' -c '1' -r '3' -cc config/tpcc.yml -cc config/client_closed.yml -cc config/chronos.yml -b tpcc -m chronos:chronos --allow-client-overlap simple

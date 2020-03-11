make && ./run_all.py -hh config/cluster.yml -s '2' -c '500' -r '3' -cc config/rw.yml -cc config/client_closed.yml -cc config/chronos.yml -b rw -m chronos:chronos --allow-client-overlap simple

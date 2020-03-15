make && ./run_all.py -hh config/cluster.yml -s '2' -c '100' -r '3' -cc config/rw.yml -cc config/client_closed.yml -cc config/brq.yml -b rw -m janus:janus --allow-client-overlap simple

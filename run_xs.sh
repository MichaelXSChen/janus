  ./run_all.py -hh config/cluster.yml -s '3' -c '3' -r '1' -z 0.4 -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_100.yml -cc config/brq.yml -b rw_benchmark -m brq:brq --allow-client-overlap testing01

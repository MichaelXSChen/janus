make && ./run_all.py -hh config/cluster.yml -s '2' -c '1' -r '3' -cc config/rw.yml -cc config/client_closed.yml -cc config/tapir.yml -b rw -m tapir:tapir --allow-client-overlap simple

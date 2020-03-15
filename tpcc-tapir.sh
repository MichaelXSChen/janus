make && ./run_all.py -hh config/cluster.yml -s '1' -c '1' -r '3' -cc config/tpcc.yml -cc config/client_closed.yml -cc config/tapir.yml -b tpcc -m tapir:tapir --allow-client-overlap simple

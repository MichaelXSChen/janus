#./waf configure build -t && ./run_all.py -hh config/cluster.yml -s '1' -c '1' -r '3' -z 0.4 -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/chronos.yml -b rw_benchmark -m chronos:chronos --allow-client-overlap testing01
./run_all.py -hh config/ov-cluster.yml -s '4' -c '4' -r '3' -z 0.4 -cc config/tpcc.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/brq.yml -b tpcc -m brq:brq --allow-client-overlap testing01
#./run_all.py -hh config/ov-cluster.yml -s '3' -c '4' -r '2' -z 0.4 -cc config/tpcc.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/brq.yml -b tpcc -m brq:brq --allow-client-overlap testing01

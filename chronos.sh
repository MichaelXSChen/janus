#./waf configure build -t && ./run_all.py -hh config/cluster.yml -s '1' -c '1' -r '3' -z 0.4 -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/chronos.yml -b rw_benchmark -m chronos:chronos --allow-client-overlap testing01
#./waf configure build -t && ./run_all.py -hh config/edge.yml -s '3' -c '3' -r '3' -z 0.4 -cc config/tpcc.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/chronos.yml -b tpcc -m chronos:chronos --allow-client-overlap testing01
./waf configure build -t && ./run_all.py -hh config/edge-seperate.yml -s '2' -c '2' -r '3' -z 0.4 -cc config/tpcc.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/chronos.yml -b tpcc -m chronos:chronos --allow-client-overlap testing01
#./waf configure build -t && ./run_all.py -hh config/cluster.yml -s '1' -c '1' -r '3' -z 0.4 -cc config/tpccd.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/chronos.yml -b tpccd -m chronos:chronos --allow-client-overlap testing01

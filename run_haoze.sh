./run_all.py -hh config/3c3s3r1p.yml -s '1:2:1' -c '9:10:1' -r '3' -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_100.yml -cc config/brq.yml -b rw -m brq:brq --allow-client-overlap testing01
tar -zxvf ./archive/testing01-rw_brq-brq_9_-1.tgz -C ./archive/
python holdlog.py

import sys
import itertools
import logging
import math
import copy

logger = logging.getLogger('')

class ClientPlacement:
    WITH_LEADER = 'with_leader'
    BALANCED = 'balanced'


class BalancedPlacementStrategy:
    def generate_layout(self, args, num_c, num_s, num_replicas, hosts_config):
        logger.debug("generate layout called")
        data_centers = args.data_centers
        logger.debug(data_centers)

        self.args = args
        self.num_c = num_c
        self.num_s = num_s
        self.num_replicas = num_replicas

        proc_names = hosts_config['host'].keys()
        # hosts = self.hosts_by_datacenter(hosts_config['host'].keys(), data_centers)
        print("procs", proc_names)
        server_sites_all = hosts_config['site']['server']
        server_sites = []
        # truncate to number of shards and number of replicas
        # This will 
        for partition in range(num_s):
            par = []
            for replica in range(num_replicas):
                if len(server_sites_all) < num_s: 
                    logging.fatal("not enough server sites (number of partitions) in config file")
                    exit(1)
                if len(server_sites_all[partition]) < num_replicas: 
                    logging.fatal("not enough server sites (number of replicas) in config file")
                    exit(1)
                par.append(server_sites_all[partition][replica])
            server_sites.append(par)

        print(server_sites)

        client_sites_all = hosts_config['site']['client']
        client_sites = []
        n_par = len(server_sites)
        if num_c > n_par:
            # reserve the positions
            for partition in range(n_par):
                client_sites.append([])
            # fill in num_c clients vertically
            col_itr = 0
            count = 0
            while col_itr < len(client_sites_all[0]):
                for partition in range(n_par):
                    if col_itr >= len(client_sites_all[partition]):
                        logging.fatal("not enough client sites (number of replicas) in config file")
                        exit(1)
                    client_sites[partition].append(client_sites_all[partition][col_itr])
                    count += 1
                    if count == num_c:
                       break
                col_itr += 1
                if count == num_c:
                    break
        else:
            # num_c < n_par    
            for i in range(num_c):
                par = []
                par.append(client_sites_all[i][0])
                client_sites.append(par)
        print(client_sites) 

        site = {'client': client_sites, 'server': server_sites}
        process_all = hosts_config['process']
        process = {}
        for s in process_all.keys():
            exist = False
            for row in client_sites:
                for c in row: 
                    if s == c:
                        exist = True 
            for row in server_sites:
                for c in row: 
                    if s == c.split(":")[0]:
                        exist = True
            if exist:
                process[s] = process_all[s]
        # process = hosts_config['process']

        print(process)
        result = {'site': site, 'process': process}
        print("result", result)
        return result

    def hosts_by_datacenter(self, hosts, data_centers):
        if len(data_centers) == 0:
            logger.debug("here")
            return {'': sorted(hosts)}
        else:
            logger.debug("hereer")
            result = {}

            for dc in data_centers:
                result[dc] = []

            for h in hosts:
                for dc in data_centers:
                    if h.find(dc)==0:
                        result[dc].append(h)
                        break

            sorted_result = { dc: sorted(value) for (dc, value) in result.iteritems() }
            return sorted_result

    def generate_process(self, process, hosts, server_names, client_names):
        num_datacenters = len(hosts.keys())
        num_servers = int(math.ceil(float(len(server_names)) / float(self.args.cpu_count)))
        num_servers_per_datacenter = int(math.ceil(float(num_servers) / float(num_datacenters)))

        logging.info("num_datacenters: %s", num_datacenters)
        logging.info("num_servers: %s", num_servers)
        logging.info("num_servers_per_datacenter: %s", num_servers_per_datacenter)

        # partition hosts in to servers and clients
        server_machines = {dc: [] for dc in hosts.keys()}
        logger.debug("server machines")
        print(server_machines)
        for dc in hosts.keys():
            server_machines[dc].extend(hosts[dc][:num_servers_per_datacenter])


        num_client_hosts = 0
        client_machines = {dc: [] for dc in hosts.keys()}
        for dc in hosts.keys():
            server_set = set(server_machines[dc])
            all_set = set(hosts[dc])
            clients = list(all_set - server_set)
            client_machines[dc].extend(clients)
            num_client_hosts += len(clients)

        # now match {server, client} names to their respective hosts

        # returns each data center in a round robin fashion -- forever
        datacenter_it = itertools.cycle(hosts.keys())

        server_hosts_it = {dc: itertools.cycle(hosts) for (dc, hosts) in server_machines.iteritems()}
        for server in server_names:
            logging.debug(server)
            dc = datacenter_it.next()
            server_host = server_hosts_it[dc].next()
            logging.info("map %s -> %s", server, server_host)
            process[server] = server_host

        datacenter_it = itertools.cycle(hosts.keys())

        if num_client_hosts == 0:
            if self.args.allow_client_overlap:
                client_machines = server_machines
            else:
                logger.error("no client machines available. exiting program.")
                logger.error("use --allow-client-overlap if testing locally.")
                sys.exit(1)

        client_hosts_it = {dc: itertools.cycle(hosts) for (dc, hosts) in client_machines.iteritems()}
        for client in client_names:
            dc = datacenter_it.next()
            client_host = client_hosts_it[dc].next()
            logging.info("map %s -> %s", client, client_host)
            process[client] = client_host


    def generate_site(self, site, server_names, client_names):
        self.generate_site_server(site, server_names)
        self.generate_site_client(site, client_names)

    def generate_site_server(self, site, server_names):
        site.update({'server': [], 'client': []})
        port = 10000
        for sid in range(self.num_s):
            row = []
            for repid in range(self.num_replicas):
                index = self.num_replicas*sid + repid
                row.append(server_names[index] + ':' + str(port))
                port += 1
            site['server'].append(row)

    def generate_site_client(self, site, client_names):
        row = []
        for cid in range(self.num_c):
            row.append(client_names[cid])
            if len(row) % (self.num_replicas) == 0 and len(row) > 0:
                site['client'].append(row)
                row = []
        if len(row) > 0:
            site['client'].append(row)


class LeaderPlacementStrategy(BalancedPlacementStrategy):
    def generate_site_client(self, site, client_names):
        row = []
        for name in client_names:
            site['client'].append([name])

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015 clowwindy
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, \
    with_statement

import sys
import os
import time
import logging
import signal
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))
from shadowsocks import shell, daemon, eventloop, tcprelay, udprelay, \
    asyncdns, manager, agent

shell.check_python()
config = shell.get_config(False)
daemon.daemon_exec(config)
agent_ = agent.Agent(config)

worker_id = 0
workers = 1

tcp_servers = []
udp_servers = []
dns_resolver = 0

#load server config
def init_config():
    global tcp_servers, udp_servers, dns_resolver, workers, worker_id
    tcp_servers = []
    udp_servers = []
    dns_resolver = asyncdns.DNSResolver()
    workers = int(config['workers'])
    worker_id = 0

    if config['port_password']:
        if config['password']:
            logging.warn('warning: port_password should not be used with '
                         'server_port and password. server_port and password '
                         'will be ignored')
    else:
        config['port_password'] = {}
        server_port = config['server_port']
        if type(server_port) == list:
            for a_server_port in server_port:
                config['port_password'][a_server_port] = config['password']
        else:
            config['port_password'][str(server_port)] = config['password']

    if config.get('manager_address', 0):
        logging.info('entering manager mode')
        manager.run(config)
        return

    port_password = config['port_password']
    #del config['port_password']
    for port, password in port_password.items():
        a_config = config.copy()
        a_config['server_port'] = int(port)
        a_config['password'] = password
        tcp_servers.append(tcprelay.TCPRelay(a_config, dns_resolver, False))
        udp_servers.append(udprelay.UDPRelay(a_config, dns_resolver, False))

    #load servers config from mysql
    mysql_servers = agent_.load_servers()
    tcp_servers += mysql_servers[0]
    udp_servers += mysql_servers[1]

def run_server():
    loop = eventloop.EventLoop()

    #default port is 0, means use random free port
    def new_server(password, port = 0):
        logging.info("starting new server on port %d."%port)
        try:
            a_config = config.copy()
            a_config['server_port'] = port
            a_config['password'] = password

            tcp_server = tcprelay.TCPRelay(a_config, dns_resolver, False)
            tcp_server.add_to_loop(loop)
            tcp_servers.append(tcp_server)

            #udp server use the same port of tcp server
            a_config['server_port'] = tcp_server._listen_port
            udp_server = udprelay.UDPRelay(a_config, dns_resolver, False)
            udp_server.add_to_loop(loop)
            udp_servers.append(udp_server)
        except Exception,e:
            shell.print_exception(e)

    #default port is 0, means destory all ports
    def destory_server(port = 0):
        logging.info("stopping server on port %d"%port)
        try:
            remove_list = []
            for server in tcp_servers + udp_servers:
                if server._listen_port == port or port == 0:
                    server.close()
                    remove_list.append(server)

            for server in remove_list:
                if server in tcp_servers:
                    tcp_servers.remove(server)
                if server in udp_servers:
                    udp_servers.remove(server)
        except Exception,e:
            logging.warn(e)

    #default port is 0, means restart all ports
    def restart_server(password, port = 0):
        logging.info("restarting server on port %d"%port)
        destory_server(port)
        time.sleep(0.1)#wait for port reuse, or will complain "Address already in use" when restart new server on the same port
        new_server(password, port)

    def child_handler(signum, _):
        logging.warn('received SIGQUIT, doing graceful shutting down..')
        list(map(lambda s: s.close(next_tick=True),
                 tcp_servers + udp_servers))
    signal.signal(getattr(signal, 'SIGQUIT', signal.SIGTERM),
                  child_handler)

    def int_handler(signum, _):
        agent_.close()  #notify thread to exit
        sys.exit(1)
    signal.signal(signal.SIGINT, int_handler)

    #subscribe msg from redis, process remote commands
    #<id>,<atcion>,[<port>],[<password>]
    #action: start|stop|restart|exit
    def msg_handler(msg):
        try:
            # logging.debug("MSG:" + str(msg))
            data = json.loads(msg['data'])
            action = data['action']
            command_id = data['id']

            if action == "start":#start new server on specified(or random) port
                # *** NOTICE ***
                #this handler will be invoked in all workers(child processes) to start new server
                #thus it will start number of servers
                #but we just need to start only one server

                port = data['port']
                password = data["password"]

                #round-robbin
                if command_id % workers == worker_id:
                    new_server(password, port)
            elif action == "stop":#stop specified(or all) server(s)
                #some ports are listened in all workers, so need to stop all of them
                port = data['port']
                destory_server(port)
            elif action == "restart":#restart specified server
                port = data['port']

                if port == 0:#restart all ports
                    #exit child process, notify parent process to restart service
                    #### NOTICE ####
                    #os.waitpid() will not get exit code using sys.exit(), we need to use os._exit()
                    # sys.exit(109)
                    destory_server(0)
                    os._exit(109)
                else:#restart specified port
                    if command_id % workers == worker_id:
                        password = data["password"]#new password for this port
                        restart_server(password, port)
            elif action == "exit":#notify all child processes to exit
                logging.info("Exiting...")

                # this function involked by redis notify thread,
                # we should notify eventloop thread to exit
                destory_server(0)
                loop.stop()
                time.sleep(eventloop.TIMEOUT_PRECISION + 0.5)

                #exit child process
                #### NOTICE ####
                #os.waitpid() will not get exit code using sys.exit(), we need to use os._exit()
                # sys.exit(0)
                os._exit(0)
            else:
                logging.warn("Invalid action:%s"%action)
        except Exception,e:
            logging.warn("Invalid msg:%s, %s"%(msg, e))

    try:
        agent_.register_command_handler(msg_handler)#will create a thread to recv commands
        dns_resolver.add_to_loop(loop)
        list(map(lambda s: s.add_to_loop(loop), tcp_servers + udp_servers))

        daemon.set_user(config.get('user', None))
        loop.run()
    except Exception as e:
        logging.error(e)
        sys.exit(1)

def main():
    global tcp_servers,udp_servers, dns_resolver, workers, worker_id
    init_config()

    need_restart = False
    if workers > 1:
        if os.name == 'posix':
            children = []
            is_child = False
            for i in range(0, workers):
                r = os.fork()
                if r == 0:
                    logging.info('worker %d started.'%worker_id)
                    is_child = True
                    run_server()
                    break
                else:
                    worker_id += 1
                    children.append(r)

            if not is_child:
                def handler(signum, _):
                    for pid in children:
                        try:
                            os.kill(pid, signum)
                            os.waitpid(pid, 0)
                        except OSError:  # child may already exited
                            pass
                    sys.exit()
                signal.signal(signal.SIGTERM, handler)
                signal.signal(signal.SIGQUIT, handler)
                signal.signal(signal.SIGINT, handler)

                # master
                for a_tcp_server in tcp_servers:
                    a_tcp_server.close()
                for a_udp_server in udp_servers:
                    a_udp_server.close()
                dns_resolver.close()

                for child in children:
                    pid, status = os.waitpid(child, 0)
                    exit_code = os.WEXITSTATUS(status)
                    logging.debug("child " + str(pid)+" exited, status:" + str(exit_code))
                    if exit_code == 109:
                        need_restart = True

                #restart all child process
                if need_restart:
                    logging.info("restarting service...")
                    main()
        else:
            logging.warn('worker is only available on Unix/Linux')
            run_server()
    else:
        run_server()

if __name__ == '__main__':
    main()

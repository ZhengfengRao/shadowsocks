import logging
import redis, redis.client
import MySQLdb

class Agent():
    def __init__(self, configure):
        try:
            #redis configure
            self._redis_host = configure['redis_host']
            self._redis_port = configure['redis_port']
            self._redis_password = configure['redis_password']
            self._redis_database = configure['redis_database']
            self._redis_channel = configure['redis_command_channel']

            self._redis = redis.StrictRedis(host=self._redis_host, port=self._redis_port,
                db=self._redis_database, password=self._redis_password, socket_timeout=None,
                socket_connect_timeout=10)
            self._redis_sub = self._redis.pubsub()
            self._handler_thread = None

            #mysql configure
            self._mysql_host = configure['mysql_host']
            self._mysql_port = configure['mysql_port']
            self._mysql_user = configure['mysql_user']
            self._mysql_password = configure['mysql_password']
            self._mysql_database = configure['mysql_database']
            self._mysql_connection = MySQLdb.connect(host = self._mysql_host,
                port = self._mysql_port, user = self._mysql_user,
                passwd = self._mysql_password, db = self._mysql_database,
                charset="utf8")

            self._mysql_connection.show_warnings()
            self._mysql_cursor = self._mysql_connection .cursor()
        except Exception, e:
            logging.error(e)

    def load_servers(self):
        tcp_servers = []
        udp_servers = []

        return tcp_servers,udp_servers

    def register_command_handler(self, handler):
        self._redis_sub.subscribe(**{self._redis_channel: handler})
        self._handler_thread = self._redis_sub.run_in_thread()

    def close(self):
        try:
            if self._handler_thread:
                self._handler_thread.stop()
            self._redis_sub.close()

            self._mysql_cursor.close()
            self._mysql_connection.commit()
            self._mysql_connection.close()
        except Exception, e:
            logging.warn(e)

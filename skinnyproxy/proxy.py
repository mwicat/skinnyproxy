from twisted.internet import reactor
from twisted.python import log
from twisted.protocols import portforward
from twisted.tap.portforward import Options

import datetime
import Queue
import plac

import threading

from sqlalchemy import *

DB_FILENAME = 'packets.db'
DB_URL = 'sqlite:///%s' % DB_FILENAME

db_worker = None

session_counter_columns = (
            Column('counter', Integer, primary_key=True),
            )

packets_columns = (
            Column('number', Integer, primary_key=True),
            Column('timestamp', DateTime),
            Column('session', Integer),
            Column('srcaddr', String),
            Column('srcport', Integer),
            Column('dstaddr', String),
            Column('dstport', Integer),
            Column('side', String),
            Column('data', Binary),
            )

def get_next_session():
    res = tbl_session_counter.select().execute()
    counter_entry = res.fetchone() 
    if counter_entry is None:
        cnt = 0
        tbl_session_counter.insert({'counter': cnt}).execute()        
    else:
        cnt = counter_entry['counter']
        cnt += 1
        tbl_session_counter.update(values={'counter': cnt}).execute()
    return cnt
    

def insert(session, data, srchost, dsthost, side):
    timestamp = datetime.datetime.now()
    entry = {'session': session,
             'timestamp': timestamp,
             'data': data,
             'srcaddr': srchost.host,
             'srcport': srchost.port,
             'dstaddr': dsthost.host,
             'dstport': dsthost.port,
             'side': side}
    i = tbl_packets.insert()
    i.execute(entry)


class LoggingProxyClient(portforward.ProxyClient):

    def dataReceived(self, data):
        db_worker.launch(insert, self.peer.session, data, self.transport.getPeer(), self.peer.transport.getPeer(), 'server')
        portforward.ProxyClient.dataReceived(self, data)


class LoggingProxyClientFactory(portforward.ProxyClientFactory):
    protocol = LoggingProxyClient

def get_client_addr(client):
    return client.transport.getPeer().host

class LoggingProxyServer(portforward.ProxyServer):
    clientProtocolFactory = LoggingProxyClientFactory

    def connectionMade(self):
        addr = get_client_addr(self)
        if addr in self.factory.blocked:
            self.transport.loseConnection()
            return
        self.session = get_next_session()
        self.factory.clients.append(self)
        portforward.ProxyServer.connectionMade(self)

    def clientConnectionFailed(self, connector, reason):
        self.factory.clients.remove(self)
    
    def clientConnectionLost(self, connector, reason):
        self.factory.clients.remove(self)
        
    def dataReceived(self, data):
        db_worker.launch(insert, self.session, data, self.transport.getPeer(), self.peer.transport.getPeer(), 'client')
        portforward.ProxyServer.dataReceived(self, data)


class LoggingProxyFactory(portforward.ProxyFactory):
    clients = []
    blocked = []
    protocol = LoggingProxyServer

    def __init__(self, host, port, db_worker):
        portforward.ProxyFactory.__init__(self, host, port)
        self.db_worker = db_worker

    def block(self, addr):
        self.blocked.append(addr)
        for client in self.clients:
            c_addr = get_client_addr(client)
            if addr == c_addr:
                client.transport.loseConnection()

    def unblock(self, addr):
        self.blocked.remove(addr)


class DBWorker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.queue = Queue.Queue()

    def launch(self, func, *args, **kwargs):
        self.queue.put((func, args, kwargs))

    def stop(self):
        self.queue.put(None)

    def run(self):
        while True:
            job = self.queue.get()
            if job is None:
                break
            func, args, kwargs = job
            func(*args, **kwargs)


from twisted.protocols.telnet import Telnet
Telnet.mode = 'Command'

from twisted.internet import reactor
from twisted.manhole import telnet


@plac.annotations(
   proxyport=('Proxy port', 'positional', None, int),
   serverport=('Server port', 'positional', None, int)
   )
def run(proxyport, serverport):
    import os
    if os.path.exists(DB_FILENAME):
        os.remove(DB_FILENAME)

    engine = create_engine(DB_URL)
    #engine.raw_connection().connection.text_factory = str

    metadata = MetaData(engine)

    global tbl_packets
    global tbl_session_counter

    tbl_packets = Table('packets', metadata, *packets_columns)
    tbl_session_counter = Table('session_counter', metadata, *session_counter_columns)

    metadata.create_all()

    global db_worker
    db_worker = DBWorker()
    db_worker.start()

    proxy_factory = LoggingProxyFactory('localhost', serverport, db_worker)
    reactor.listenTCP(proxyport, proxy_factory)


    factory = telnet.ShellFactory()
    port = reactor.listenTCP( 8787, factory)
    factory.namespace['proxy'] = proxy_factory
    factory.username = 'guest'
    factory.password = 'guest'


    reactor.run()
    db_worker.stop()


def main():
    plac.call(run)

if __name__ == '__main__':
    main()

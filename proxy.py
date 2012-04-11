from twisted.internet import reactor
from twisted.python import log
from twisted.protocols import portforward
from twisted.tap.portforward import Options

import datetime

import plac


from sqlalchemy import *

DB_FILENAME = 'packets.db'
DB_URL = 'sqlite:///%s' % DB_FILENAME


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

    srcport = srchost.host
    
    entry = {'session': session, 'timestamp': timestamp, 'data': data, 'srcaddr': srchost.host, 'srcport': srchost.port, 'dstaddr': dsthost.host, 'dstport': dsthost.port, 'side': side}
    i = tbl_packets.insert()
    i.execute(entry)
    #print 'inserting', entry

class LoggingProxyClient(portforward.ProxyClient):

    def dataReceived(self, data):
        #log.msg('server sent: ' + repr(data))

        insert(self.peer.session, data, self.transport.getPeer(), self.peer.transport.getPeer(), 'server')
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
        srchost = self.transport.getHost()
        srcaddr = srchost.host
        srcport = srchost.port

        #log.msg('client sent: ' + repr(data))

        insert(self.session, data, self.transport.getPeer(), self.peer.transport.getPeer(), 'client')

        portforward.ProxyServer.dataReceived(self, data)

    # def __str__(self):
        


class LoggingProxyFactory(portforward.ProxyFactory):
    clients = []
    blocked = []
    protocol = LoggingProxyServer

    def block(self, addr):
        self.blocked.append(addr)
        for client in self.clients:
            c_addr = get_client_addr(client)
            if addr == c_addr:
                client.transport.loseConnection()

    def unblock(self, addr):
        self.blocked.remove(addr)

from twisted.protocols.telnet import Telnet
Telnet.mode = 'Command'

from twisted.internet import reactor
from twisted.manhole import telnet


@plac.annotations(
   proxyport=('Proxy port', 'positional', None, int),
   serverport=('Server port', 'positional', None, int)
   )
def main(proxyport, serverport):
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

    proxy_factory = LoggingProxyFactory('localhost', serverport)
    reactor.listenTCP(proxyport, proxy_factory)


    factory = telnet.ShellFactory()
    port = reactor.listenTCP( 8787, factory)
    factory.namespace['proxy'] = proxy_factory
    factory.username = 'guest'
    factory.password = 'guest'

    reactor.run()

# this only runs if the module was *not* imported
if __name__ == '__main__':
    plac.call(main)

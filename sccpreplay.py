from sqlalchemy import *

import datetime

from itertools import groupby
from operator import itemgetter

DB_URL = 'sqlite:///packets.db'

import proxy

import plac

from twisted.internet import reactor, protocol

def get_packet_delay(packet):
    return packet['deltatime'].total_seconds()

class EchoClient(protocol.Protocol):
    """Once connected, send a message, then print the result."""
    
    def connectionMade(self):
        self.scheduled = [reactor.callLater(get_packet_delay(packet), self.sendString, str(packet['data'])) for packet in self.factory.packets]

    def sendString(self, data):
        print 'sending data'
        self.transport.write(data)    

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        print "connection lost"

class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient

    def __init__(self, packets):
        self.packets = packets
        #print self.packets

    def clientConnectionFailed(self, connector, reason):
        print "Connection failed - goodbye!"
    
    def clientConnectionLost(self, connector, reason):
        print "Connection lost - goodbye!"

def get_packets(packet_filter=None):
    sql_filter = '%s and ' % packet_filter if packet_filter is not None else ''
    sql_filter += 'side="client"'
    packets_result = tbl_packets.select(sql_filter).execute()
    packets = [dict(p) for p in packets_result]
    if not packets:
        return packets
    timestamp_first = packets[0]['timestamp']
    packets[0]['deltatime'] = datetime.timedelta(0)
    for packet in packets[1:]:
        packet['deltatime'] = packet['timestamp'] - timestamp_first
    packets = groupby(packets, itemgetter('session'))
    return packets

    
@plac.annotations(
   filter=('SQL filter', 'option', 'f'))
def main(filter=None):
    engine = create_engine(DB_URL)
    #engine.raw_connection().connection.text_factory = str

    metadata = MetaData(engine)

    global tbl_packets
    global tbl_session_counter

    tbl_packets = Table('packets', metadata, *proxy.packets_columns)
    tbl_session_counter = Table('session_counter', metadata, *proxy.session_counter_columns)

    metadata.create_all()

    packets_by_session = get_packets(filter)
    for session, packets in packets_by_session:
        #print session
        packets = list(packets)
        factory = EchoFactory(packets)
        packet_first = packets[0]
        dstaddr = packet_first['dstaddr']
        dstport = packet_first['dstport']
        reactor.connectTCP(dstaddr, dstport, factory)

    reactor.run()

# this only runs if the module was *not* imported
if __name__ == '__main__':
    plac.call(main)

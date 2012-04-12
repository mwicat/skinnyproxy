import sys; sys.path.append('../skinnygen/src')

from sqlalchemy import *

import datetime

from itertools import groupby
from operator import itemgetter
from collections import defaultdict

from network import sccpclientprotocol
from sccp import messagefactory

DB_URL = 'sqlite:///packets.db'

import reassemble

import model

import plac

from twisted.internet import reactor, protocol

def localhost_ip_gen():
    return ('127.%d.%d.%d' % (x, y, z) for x in range(255) for y in range(255) for z in range(2, 255))

def get_packet_delay(packet):
    return packet['deltatime'].total_seconds()

def append_delta(packets):
    timestamp_first = packets[0]['timestamp']
    packets[0]['deltatime'] = datetime.timedelta(0)
    for packet in packets[1:]:
        packet['deltatime'] = packet['timestamp'] - timestamp_first
    return packets


def get_packets(packet_filter=None):
    sql_filter = '%s' % packet_filter if packet_filter is not None else ''
    packets_result = tbl_packets.select(sql_filter).execute()
    packets = [dict(p) for p in packets_result]
    return packets


def group_packets_by_session(packets):
    res = defaultdict(list)
    for packet in packets:
        k = packet['session']
        res[k].append(packet)
    return res.items()

def is_client_packet(packet):
    return packet['side'] == 'client'

class EchoClient(protocol.Protocol):
    """Once connected, send a message, then print the result."""

    message_factory = messagefactory.MessageFactory()

    def __init__(self):
        self.assembler = reassemble.MessageAssembler()
    
    def connectionMade(self):
        self.processPacketsToSend()
        self.scheduled = []
    
    def deserialize(self, packet_data):
        return sccpclientprotocol.deserialize(packet_data, self.message_factory)

    def logMessage(self, message, operation_name):
        operations_symbols = {'send': '>>',
                              'schedule': '**',
                              'expect': '&&',
                              'receive': '<<'}
        symbol = operations_symbols.get(operation_name, operation_name)
        meta = {}
        addr = self.transport.getHost().host
        if hasattr(message, 'callId'):
            meta['callId'] = message.callId
        print addr, symbol, message, meta, '%04X' % message.sccpmessageType
    
    def processPacketsToSend(self):
        processing = True
        while processing and self.packets:
            packet = self.packets.pop(0)
            packet_data = packet['data']
            message = self.deserialize(packet_data)
            sending = is_client_packet(packet)

            if sending:
                reactor.callLater(get_packet_delay(packet), self.sendString, packet_data)
                self.logMessage(message, 'schedule')
            else:
                self.logMessage(message, 'expect')

    def sendMessage(self,message):
        self.sendString(message.pack())

    def sendString(self, packet_data):
        if packet_data is not None:
            message = self.deserialize(packet_data)
            self.logMessage(message, 'send')
        self.transport.write(sccpclientprotocol.to_frame(packet_data, sccpclientprotocol.SCCPClientProtocol.structFormat))

    def dataReceived(self, data):
        packets_data = self.assembler.feed(data)
        for packet_data in packets_data:
            message = self.deserialize(packet_data)
            self.logMessage(message, 'receive')

    def connectionLost(self, reason):
        print "connection lost"


class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient

    def __init__(self, packets):
        self.packets = packets
        #print self.packets

    def buildProtocol(self, *args, **kw):
        prot = EchoClient()
        prot.packets = self.packets[:]
        return prot


    def clientConnectionFailed(self, connector, reason):
        print "Connection failed - goodbye!"
    
    def clientConnectionLost(self, connector, reason):
        print "Connection lost - goodbye!"



    
@plac.annotations(
   filter=('SQL filter', 'option', 'f'))
def run(filter=None):
    engine = create_engine(DB_URL)

    metadata = MetaData(engine)

    global tbl_packets
    global tbl_session_counter

    tbl_packets = Table('messages', metadata, *model.messages_columns)
    tbl_session_counter = Table('session_counter', metadata, *model.session_counter_columns)

    metadata.create_all()

    packets = get_packets(filter)
    packets = append_delta(packets)
    packets_by_session = group_packets_by_session(packets)

    ip_gen = localhost_ip_gen()

    for session, packets in packets_by_session:
        packets = list(packets)
        factory = EchoFactory(packets)
        packet_first = packets[0]
        dstaddr = packet_first['dstaddr']
        dstport = packet_first['dstport']
        print dstaddr, dstport
        client_ip = ip_gen.next()
        bindAddress=(client_ip, 0)
        reactor.connectTCP(dstaddr, dstport, factory, bindAddress=bindAddress)

    reactor.run()


def main():
    plac.call(run)

if __name__ == '__main__':
    main()

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


def get_call_id(message):
    return getattr(message, 'callId', None)

def get_effective_call_id(message):
    call_id = get_call_id(message)
    return call_id if call_id != 0 else None

import subprocess

def injectHex(addr, data_hex):
    args = ['./proxy_send.sh', "proxy.injectHex('%s', '%s')" % (addr, data_hex)]
    subprocess.call(args)

class EchoClient(protocol.Protocol):
    """Once connected, send a message, then print the result."""

    message_factory = messagefactory.MessageFactory()

    def __init__(self):
        self.assembler = reassemble.MessageAssembler()
        self.call_replacements = {}
        self.unresolved_call_ids = []
        self.new_call_ids_replaced = set()
    
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
                call_id = get_effective_call_id(message)
                if call_id is not None and call_id not in self.unresolved_call_ids:
                    self.unresolved_call_ids.append(call_id)

    def sendMessage(self,message):
        self.sendString(message.pack())

    def sendString(self, message_data):
        message = self.deserialize(message_data)
        self.logMessage(message, 'send')

        call_id = get_effective_call_id(message)
        if call_id is not None:
            message.callId = self.call_replacements[message.callId]
            message_data_to_send = message.pack()
        else:
            message_data_to_send = message_data

        frame_to_send = sccpclientprotocol.to_frame(message_data_to_send, sccpclientprotocol.SCCPClientProtocol.structFormat)
        self.transport.write(frame_to_send)


    def maybeAddReplacement(self, call_id):
        if call_id is None:
            return
        if call_id not in self.new_call_ids_replaced:
            self.new_call_ids_replaced.add(call_id)
            if self.unresolved_call_ids:
                call_id_to_replace = self.unresolved_call_ids.pop(0)
                self.call_replacements[call_id_to_replace] = call_id

    def dataReceived(self, data):
        packets_data = self.assembler.feed(data)
        for packet_data in packets_data:
            message = self.deserialize(packet_data)
            call_id = get_effective_call_id(message)
            self.maybeAddReplacement(call_id)
            self.logMessage(message, 'receive')

        addr_to_inject = self.factory.srcaddr
        injectHex(addr_to_inject, data.encode('hex'))

    def connectionLost(self, reason):
        print "connection lost"


class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient

    def __init__(self, packets, srcaddr, srcport):
        self.packets = packets
        self.srcaddr = srcaddr
        self.srcport = srcport
        #print self.packets

    def buildProtocol(self, *args, **kw):
        prot = EchoClient()
        prot.factory = self
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
        packet_first = packets[0]

        srcaddr = packet_first['srcaddr']
        srcport = packet_first['srcport']
        dstaddr = packet_first['dstaddr']
        dstport = packet_first['dstport']

        factory = EchoFactory(packets, srcaddr, srcport)
        client_ip = ip_gen.next()
        bindAddress=(client_ip, 0)
        reactor.connectTCP(dstaddr, dstport, factory, bindAddress=bindAddress)

    reactor.run()


def main():
    plac.call(run)

if __name__ == '__main__':
    main()

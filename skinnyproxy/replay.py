import datetime
from itertools import groupby
from operator import itemgetter
from collections import defaultdict

import plac
from sqlalchemy import *

import sys; sys.path.append('../skinnygen/src')
from network import sccpclientprotocol
from sccp import messagefactory

import reassemble
import model

from twisted.protocols.telnet import Telnet
Telnet.mode = 'Command'

from twisted.internet import reactor
from twisted.manhole import telnet


DB_URL = 'sqlite:///packets.db'

injections = []

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


def get_packets(tbl_packets, packet_filter=None):
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

def get_party_id(message):
    return getattr(message, 'passThruPartyId', None)

import subprocess

def inject(addr, data):
    injectHex(addr, data.encode('hex'))

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

        self.party_replacements = {}
        self.unresolved_party_ids = []
        self.new_party_ids_replaced = set()

    
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
        party_id = get_party_id(message)

        if call_id is not None or party_id is not None:
            if call_id is not None:
                message.callId = self.call_replacements[message.callId]
            if party_id is not None:
                message.passThruPartyId = self.call_replacements[message.passThruPartyId]
            message_data_to_send = message.pack()
        else:
            message_data_to_send = message_data

        frame_to_send = sccpclientprotocol.to_frame(message_data_to_send, sccpclientprotocol.SCCPClientProtocol.structFormat)
        self.transport.write(frame_to_send)


    def maybeAddReplacement(self, attr, attrs_unresolved, attrs_replaced, attrs_replacements):
        if attr is None:
            return
        if attr not in attrs_replaced:
            if attrs_unresolved:
                attr_to_replace = attrs_unresolved.pop(0)
                attrs_replacements[attr_to_replace] = attr
            attrs_replaced.add(attr)

    def dataReceived(self, data):
        packets_data = self.assembler.feed(data)
        for packet_data in packets_data:
            message = self.deserialize(packet_data)

            call_id = get_effective_call_id(message)
            self.maybeAddReplacement(call_id, self.unresolved_call_ids, self.new_call_ids_replaced, self.call_replacements)

            party_id = get_party_id(message)
            self.maybeAddReplacement(party_id, self.unresolved_party_ids, self.new_party_ids_replaced, self.party_replacements)

            self.logMessage(message, 'receive')

            if self.factory.inject:
                addr_to_inject = self.factory.srcaddr
                framed_data = sccpclientprotocol.to_frame(packet_data, sccpclientprotocol.SCCPClientProtocol.structFormat)
                if self.factory.debug:
                    injection = addr_to_inject, framed_data, message
                    injections.append(injection)
                else:
                    inject(addr_to_inject, framed_data)

    def connectionLost(self, reason):
        print "connection lost"

def step():
    if injections:
        addr, framed_data, message = injections.pop(0)
        if injections:
            print injections[0]
        inject(addr, framed_data)

class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient

    def __init__(self, packets, srcaddr, srcport, inject=False, debug=False):
        self.packets = packets
        self.srcaddr = srcaddr
        self.srcport = srcport
        self.inject = inject
        self.debug = debug
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
   filter=('SQL filter', 'option', 'f'),
   inject=('Do proxy injection', 'flag', 'i'),
   debug=('Debug mode', 'flag', 'd')
   )
def run(filter=None, inject=False, debug=False):
    engine = create_engine(DB_URL)

    metadata = MetaData(engine)

    global tbl_packets
    global tbl_session_counter

    tbl_packets = Table('messages', metadata, *model.messages_columns)
    tbl_session_counter = Table('session_counter', metadata, *model.session_counter_columns)

    metadata.create_all()

    packets = get_packets(tbl_packets, filter)
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

        factory = EchoFactory(packets, srcaddr, srcport, inject=inject, debug=debug)
        client_ip = ip_gen.next()
        bindAddress=(client_ip, 0)
        reactor.connectTCP(dstaddr, dstport, factory, bindAddress=bindAddress)

    factory = telnet.ShellFactory()
    port = reactor.listenTCP( 8989, factory)
    factory.namespace['injections'] = injections
    factory.namespace['step'] = step
    factory.username = 'guest'
    factory.password = 'guest'


    reactor.run()


def main():
    plac.call(run)

if __name__ == '__main__':
    main()

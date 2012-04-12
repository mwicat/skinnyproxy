from sqlalchemy import *
import skinnyproxy

from skinnyproxy import sccpreplay, model

import sys; sys.path.append('../skinnygen/src')
from network import sccpclientprotocol
from sccp.messagefactory import MessageFactory

from collections import defaultdict

import plac
import struct


class MessageAssembler:

    structFormat = "<L"
    prefixLength = struct.calcsize(structFormat)
    trailingNbOfBytes = 4
    MAX_LENGTH = 99999

    def __init__(self):
        self.messageFactory = MessageFactory()
        self.recvd = ""
    
    def feed(self, recd):
        self.recvd = self.recvd + recd
        packets_data = []
        while len(self.recvd) >= self.prefixLength:
            length ,= struct.unpack(
                self.structFormat, self.recvd[:self.prefixLength])
            length=length+self.trailingNbOfBytes
            
            if length > self.MAX_LENGTH:
                raise Exception('Length limit exceeded')
            if len(self.recvd) < length + self.prefixLength:
                break

            data = self.recvd[self.prefixLength:length + self.prefixLength]
            self.recvd = self.recvd[length + self.prefixLength:]
            packets_data.append(data)
            
        return packets_data


def packet_to_message(packet):
    message = packet.copy()
    del message['number']
    del message['data']
    return message

@plac.annotations(
   filter=('SQL filter', 'option', 'f'))
def run(filter=None):
    engine = create_engine(sccpreplay.DB_URL)
    metadata = MetaData(engine)

    tbl_packets = Table('packets', metadata, *model.packets_columns)
    tbl_messages = Table('messages', metadata, *model.messages_columns)
    tbl_session_counter = Table('session_counter', metadata, *model.session_counter_columns)

    metadata.create_all()
    tbl_messages.delete().execute()

    packets = tbl_packets.select().execute()

    messages = []

    i = 1
    j = 1

    messages = []
    message_factory = MessageFactory()

    channels_assemblers = defaultdict(MessageAssembler)
    
    for packet in packets:
        channel = (packet['session'], packet['side'])
        assembler = channels_assemblers[channel]
        packets_data = assembler.feed(packet.data)

        for packet_data in packets_data:
            # if packet_data[4] == '\x03':
            #     print 'button'
            message = packet_to_message(dict(packet))
            message['data'] = packet_data
            m = sccpclientprotocol.deserialize(packet_data, message_factory)
            message['type'] = m.sccpmessageType
            # print packet['number'], message['session'], message['side'], m, '%04X' % m.sccpmessageType
            messages.append(message)

    tbl_messages.insert().execute(messages)

def main():
    plac.call(run)

if __name__ == '__main__':
    main()

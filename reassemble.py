from sqlalchemy import *
import skinnyproxy

from skinnyproxy import sccpreplay, model

import sys; sys.path.append('../skinnygen/src')

from network import sccpclientprotocol
from sccp.messagefactory import MessageFactory

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
        """
        Convert int prefixed strings into calls to stringReceived.
        """
        self.recvd = self.recvd + recd
        while len(self.recvd) >= self.prefixLength:
            length ,= struct.unpack(
                self.structFormat, self.recvd[:self.prefixLength])
            length=length+self.trailingNbOfBytes
            
            if length > self.MAX_LENGTH:
                raise Exception('Length limit exceeded')
            if len(self.recvd) < length + self.prefixLength:
                break

            packet_data = self.recvd[self.prefixLength:length + self.prefixLength]
            self.recvd = self.recvd[length + self.prefixLength:]

            return packet_data


def packet_to_message(packet):
    message = packet.copy()
    del message['number']
    del message['data']
    return message

@plac.annotations(
   filter=('SQL filter', 'option', 'f'))
def main(filter=None):
    assembler = MessageAssembler()

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
    data = ''
    for packet in packets:
        data += packet.data
        packet_data = assembler.feed(packet.data)
        if packet_data is not None:
            message = packet_to_message(dict(packet))
            message['data'] = data
            m = sccpclientprotocol.deserialize(packet_data, MessageFactory())
            print m
            messages.append(message)
            data = ''

    tbl_messages.insert().execute(messages)

if __name__ == '__main__':
    plac.call(main)

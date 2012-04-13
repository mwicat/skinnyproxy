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

import replay


@plac.annotations(
   filter=('SQL filter', 'option', 'f'))
def run(filter=None):
    engine = create_engine(replay.DB_URL)
    metadata = MetaData(engine)

    tbl_packets = Table('messages', metadata, *model.messages_columns)
    tbl_session_counter = Table('session_counter', metadata, *model.session_counter_columns)

    packets = replay.get_packets(tbl_packets, filter)

    for packet in packets:
        print packet
        packet_data = packet['data']
        framed_data = sccpclientprotocol.to_frame(packet_data, sccpclientprotocol.SCCPClientProtocol.structFormat)
        replay.inject(packet['dstaddr'], framed_data)


def main():
    plac.call(run)

if __name__ == '__main__':
    main()


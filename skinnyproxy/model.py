from sqlalchemy import *

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

messages_columns = (
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


session_counter_columns = (
            Column('counter', Integer, primary_key=True),
            )


## Installation

sudo apt-get install python python-pip python-twisted
sudo pip install -e git+git://github.com/mwicat/skinnygen.git#egg=skinnygen
sudo pip install -e git+git://github.com/mwicat/skinnyproxy.git#egg=skinnyproxy


## Show filter parameters

    sqlite3 packets.db '.schema packets'
    
## Browsing database

    sqlite3 packets.db 'select * from packets'

## Proxy
    
    sccpproxy

## Proxy with detach
    
    sccpproxy -d 192.168.0.10,192.168.0.11,192.168.0.12

## Detaching

    ./proxy_detach.sh 192.168.0.10

## Injection

    sccpinject -f "type = $((0x9b)) and session = 53"
    
## Replay with injection

    sccpreplay -f 'session in (208,209,210)' -i
